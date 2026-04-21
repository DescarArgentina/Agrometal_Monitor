using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using System.Linq;

namespace DescarConector.WindowsService;

internal enum TipoArchivo { Mbom, Contexto }

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ConectorSettings _cfg;

    private FileSystemWatcher? _watcher;

    // Canal para Contextos (flujo simple: sin claim/secuencia)
    private readonly Channel<string> _contextoQueue = Channel.CreateUnbounded<string>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    private readonly ConcurrentDictionary<string, byte> _contextoEnqueued = new(StringComparer.OrdinalIgnoreCase);

    // Slot de procesamiento: garantiza que MBOM y Contexto nunca corran al mismo tiempo.
    // Contextos tienen prioridad: el loop MBOM espera mientras _contextosActivos > 0.
    private readonly SemaphoreSlim _processingSlot = new(1, 1);
    private int _contextosActivos = 0;

    // Canal "raw" MBOM: entradas del watcher + rescans (sin cambios)
    private readonly Channel<string> _incoming = Channel.CreateUnbounded<string>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

    // *** CAMBIO CLAVE ***
    // Reemplazamos Channel<string> _queue por un semáforo binario.
    // La cola real VIVE EN EL DISCO (carpetas en _CLAIMED con prefijo numérico).
    // MaxCount=1: señales redundantes se descartan automáticamente.
    private readonly SemaphoreSlim _queueSignal = new(0, 1);

    // Contador de secuencia para nombrar las carpetas de staging.
    // Las carpetas se llaman "000001_xxxxxxxx", "000002_xxxxxxxx", etc.
    // El orden de procesamiento es el orden lexicográfico de esos nombres,
    // por lo que RENOMBRAR UNA CARPETA = CAMBIAR SU POSICIÓN EN LA COLA.
    private long _secuencia = 0;

    // Evita intentar reclamar el mismo archivo múltiples veces por eventos duplicados
    private readonly ConcurrentDictionary<string, byte> _claiming = new(StringComparer.OrdinalIgnoreCase);

    private string _claimedDir = string.Empty;

    // Archivo de visibilidad de cola: se reescribe cada vez que la cola cambia.
    // El operador puede abrirlo en cualquier momento para ver qué sigue.
    private const string ArchivoProximo = "_proximo.txt";

    private static readonly TimeSpan RescanInterval = TimeSpan.FromSeconds(15);

    public Worker(ILogger<Worker> logger, IOptions<ConectorSettings> options)
    {
        _logger = logger;
        _cfg = options.Value;
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        ValidarConfiguracion();

        Directory.CreateDirectory(_cfg.MbomPendientesPath);
        Directory.CreateDirectory(_cfg.ProcesadosRootPath);
        Directory.CreateDirectory(_cfg.ContextoProcesadosRootPath);

        _claimedDir = Path.Combine(_cfg.MbomPendientesPath, "_CLAIMED");
        Directory.CreateDirectory(_claimedDir);

        _logger.LogInformation("Servicio iniciado. Monitoreando: {Path} | Prefijo Contexto: '{Prefix}'",
            _cfg.MbomPendientesPath, _cfg.ContextoFilePrefix);
        _logger.LogInformation("Carpeta CLAIMED: {Path}", _claimedDir);

        // Inicializar el contador de secuencia leyendo las carpetas existentes,
        // para que si el servicio se reinicia no reutilice números ya usados.
        _secuencia = InicializarSecuencia();
        _logger.LogInformation("Secuencia de cola iniciada en: {Seq}", _secuencia);

        // 1) Retomar backlog ya reclamado (por si el servicio se cayó)
        EncolarClaimedBacklog();

        // 2) Publicar backlog pendiente MBOM (sin reclamar) para que el claim loop lo capture
        EncolarBacklog();

        // 3) Publicar backlog pendiente de Contextos
        EncolarContextoBacklog();

        // Escribir el estado inicial de la cola en _proximo.txt
        ActualizarProximo();

        _watcher = new FileSystemWatcher(_cfg.MbomPendientesPath, "*.plmxml")
        {
            IncludeSubdirectories = false,
            EnableRaisingEvents = true,
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.Size | NotifyFilters.LastWrite | NotifyFilters.CreationTime,
            InternalBufferSize = 64 * 1024
        };

        _watcher.Created += (_, e) => PublishIncoming(e.FullPath, "Created");
        _watcher.Renamed += (_, e) => PublishIncoming(e.FullPath, "Renamed");
        _watcher.Changed += (_, e) => PublishIncoming(e.FullPath, "Changed");

        _watcher.Error += (_, e) =>
        {
            _logger.LogError(e.GetException(), "FileSystemWatcher error. Forzando rescan.");
            try { EncolarBacklog(); EncolarContextoBacklog(); }
            catch (Exception ex) { _logger.LogError(ex, "Error durante rescan en handler Error."); }
        };

        return base.StartAsync(cancellationToken);
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _watcher?.Dispose();
        _watcher = null;
        _logger.LogInformation("Servicio detenido.");
        return base.StopAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var claimTask   = ClaimLoopAsync(stoppingToken);
        var processTask = ProcessLoopAsync(stoppingToken);
        var rescanTask  = PeriodicRescanAsync(stoppingToken);
        var contextoTask = ContextoLoopAsync(stoppingToken);

        await Task.WhenAll(claimTask, processTask, rescanTask, contextoTask);
    }

    // =========================================================================
    // HELPERS DE COLA / SECUENCIA
    // =========================================================================

    /// <summary>
    /// Lee las carpetas existentes en _CLAIMED y devuelve el número de secuencia
    /// más alto encontrado (para continuar desde ahí al reiniciar).
    /// </summary>
    private long InicializarSecuencia()
    {
        if (!Directory.Exists(_claimedDir)) return 0;

        long max = 0;
        foreach (var dir in Directory.EnumerateDirectories(_claimedDir))
        {
            var name = new DirectoryInfo(dir).Name;
            var idx  = name.IndexOf('_');
            if (idx > 0 && long.TryParse(name.AsSpan(0, idx), out var num))
                max = Math.Max(max, num);
        }
        return max;
    }

    /// <summary>
    /// Señala que hay al menos un item en la cola de disco.
    /// Si ya hay una señal pendiente, la descarta (no acumula).
    /// </summary>
    private void SignalQueue()
    {
        try { _queueSignal.Release(); }
        catch (SemaphoreFullException) { /* señal ya estaba pendiente, no hace falta otra */ }
    }

    /// <summary>
    /// Devuelve el path del próximo .plmxml a procesar, determinado por
    /// el orden lexicográfico de las CARPETAS dentro de _CLAIMED.
    /// La carpeta con prefijo numérico más bajo es la primera.
    /// </summary>
    private string? ObtenerSiguienteClaimedPath()
    {
        if (!Directory.Exists(_claimedDir)) return null;

        return Directory
            .EnumerateFiles(_claimedDir, "*.plmxml", SearchOption.AllDirectories)
            .OrderBy(f => Path.GetDirectoryName(f), StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault();
    }

    /// <summary>
    /// Reescribe _CLAIMED\_proximo.txt con el estado actual de la cola.
    /// El operador puede abrir este archivo en cualquier momento para ver
    /// cuál es el próximo y cuántos hay pendientes.
    /// Para cambiar el orden: renombrar la carpeta padre del archivo
    /// cambiando su prefijo numérico (ej: "000005_xxxx" -> "000001_xxxx").
    /// </summary>
    private void ActualizarProximo()
    {
        try
        {
            var todos = Directory
                .EnumerateFiles(_claimedDir, "*.plmxml", SearchOption.AllDirectories)
                .OrderBy(f => Path.GetDirectoryName(f), StringComparer.OrdinalIgnoreCase)
                .ToList();

            var rutaArchivo = Path.Combine(_claimedDir, ArchivoProximo);
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"Actualizado: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine();

            if (todos.Count == 0)
            {
                sb.AppendLine("Cola vacía. No hay MBOMs pendientes de procesar.");
            }
            else
            {
                sb.AppendLine($"Cola: {todos.Count} MBOM(s) pendiente(s)");
                sb.AppendLine(new string('-', 70));

                for (int i = 0; i < todos.Count; i++)
                {
                    var prefijo = i == 0
                        ? "  [PRÓXIMO] -->"
                        : $"  [{i + 1,6}]     ";
                    var carpeta = Path.GetFileName(Path.GetDirectoryName(todos[i]) ?? "");
                    var archivo = Path.GetFileName(todos[i]);
                    sb.AppendLine($"{prefijo}  Carpeta: {carpeta}  |  Archivo: {archivo}");
                }

                sb.AppendLine();
                sb.AppendLine(new string('-', 70));
                sb.AppendLine("Para cambiar el orden, renombrá la carpeta padre cambiando");
                sb.AppendLine("su prefijo numérico. Ejemplos:");
                sb.AppendLine("  Adelantar:  000005_xxxx  ->  000001_xxxx");
                sb.AppendLine("  Retrasar:   000002_xxxx  ->  000099_xxxx");
                sb.AppendLine("El servicio respeta el orden lexicográfico de los nombres de carpeta.");
            }

            File.WriteAllText(rutaArchivo, sb.ToString(), System.Text.Encoding.UTF8);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "No se pudo actualizar {Archivo}", ArchivoProximo);
        }
    }

    // =========================================================================
    // VALIDACIÓN DE CONFIGURACIÓN (sin cambios respecto al original)
    // =========================================================================

    private void ValidarConfiguracion()
    {
        void MustExist(string path, string name)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new InvalidOperationException($"Configuración inválida: {name} vacío.");

            if (name.Contains("Exe", StringComparison.OrdinalIgnoreCase) && !File.Exists(path))
                throw new FileNotFoundException($"No existe el ejecutable configurado: {name} = {path}", path);
        }

        MustExist(_cfg.MbomPendientesPath, nameof(_cfg.MbomPendientesPath));
        MustExist(_cfg.ProcesadosRootPath, nameof(_cfg.ProcesadosRootPath));
        MustExist(_cfg.ExeIntermedioPath,  nameof(_cfg.ExeIntermedioPath));
        MustExist(_cfg.ExePrincipalPath,   nameof(_cfg.ExePrincipalPath));

        MustExist(_cfg.ContextoProcesadosRootPath,   nameof(_cfg.ContextoProcesadosRootPath));
        MustExist(_cfg.ExeIntermedioContextoPath,    nameof(_cfg.ExeIntermedioContextoPath));
        MustExist(_cfg.ExePrincipalContextoPath,     nameof(_cfg.ExePrincipalContextoPath));

        if (string.IsNullOrWhiteSpace(_cfg.TcxmlExportDir))
            throw new InvalidOperationException("Configuración inválida: TcxmlExportDir vacío.");

        if (string.IsNullOrWhiteSpace(_cfg.ContextoFilePrefix))
            throw new InvalidOperationException("Configuración inválida: ContextoFilePrefix vacío.");

        if (_cfg.FileReadyTimeoutSeconds <= 0) _cfg.FileReadyTimeoutSeconds = 600;
        if (_cfg.FileReadyPollMs <= 0)         _cfg.FileReadyPollMs = 500;
    }

    // =========================================================================
    // WATCHER / BACKLOG (sin cambios respecto al original)
    // =========================================================================

    /// <summary>
    /// Determina si un archivo .plmxml es de Contexto según el prefijo configurado.
    /// </summary>
    private bool EsContexto(string fullPath)
    {
        if (string.IsNullOrWhiteSpace(_cfg.ContextoFilePrefix)) return false;
        var nombre = Path.GetFileName(fullPath);
        return nombre.StartsWith(_cfg.ContextoFilePrefix, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Punto de entrada de todos los eventos del watcher.
    /// Ruteá a Contexto o MBOM según el nombre del archivo.
    /// </summary>
    private void PublishIncoming(string fullPath, string reason)
    {
        if (string.IsNullOrWhiteSpace(fullPath)) return;

        if (EsContexto(fullPath))
        {
            TryEnqueueContexto(fullPath, reason);
        }
        else
        {
            _incoming.Writer.TryWrite(fullPath);
            _logger.LogInformation("Detectado MBOM ({Reason}): {File}", reason, fullPath);
        }
    }

    private void EncolarBacklog()
    {
        var files = Directory
            .EnumerateFiles(_cfg.MbomPendientesPath, "*.plmxml", SearchOption.TopDirectoryOnly)
            .Where(f => !EsContexto(f))
            .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (files.Count > 0)
            _logger.LogInformation("Backlog MBOM: {Count} archivos .plmxml", files.Count);

        foreach (var f in files)
            PublishIncoming(f, "Backlog");
    }

    private void EncolarClaimedBacklog()
    {
        if (string.IsNullOrWhiteSpace(_claimedDir) || !Directory.Exists(_claimedDir)) return;

        var files = Directory
            .EnumerateFiles(_claimedDir, "*.plmxml", SearchOption.AllDirectories)
            .OrderBy(f => Path.GetDirectoryName(f), StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (files.Count > 0)
        {
            _logger.LogInformation("Backlog detectado en CLAIMED: {Count} archivos .plmxml", files.Count);
            // No hace falta encolar uno por uno: con una señal alcanza.
            // ProcessLoopAsync drenará todos los disponibles antes de volver a esperar.
            SignalQueue();
        }
    }

    // =========================================================================
    // LOOPS
    // =========================================================================

    private async Task ClaimLoopAsync(CancellationToken ct)
    {
        await foreach (var rawPath in _incoming.Reader.ReadAllAsync(ct))
        {
            try
            {
                var claimed = await TryClaimAsync(rawPath, ct);
                if (claimed is null) continue;

                _logger.LogInformation("Encolado (CLAIMED): {File}", claimed);
                ActualizarProximo(); // actualizar visibilidad de cola
                SignalQueue();       // notificar al ProcessLoop
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex) { _logger.LogError(ex, "Error en ClaimLoop con archivo: {File}", rawPath); }
        }
    }

    private async Task ProcessLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // Esperar señal de "hay algo en la cola de disco"
            try { await _queueSignal.WaitAsync(ct); }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }

            // Drena TODOS los archivos disponibles antes de volver a esperar la señal.
            // Esto garantiza que si llegan varios mientras se procesa uno, no se pierden.
            while (!ct.IsCancellationRequested)
            {
                var next = ObtenerSiguienteClaimedPath();
                if (next is null) break; // cola vacía, volver a esperar señal

                // Esperar a que no haya contextos pendientes ni en proceso
                while (Volatile.Read(ref _contextosActivos) > 0 && !ct.IsCancellationRequested)
                {
                    _logger.LogInformation("MBOM en espera: hay {Count} contexto(s) activo(s).", Volatile.Read(ref _contextosActivos));
                    await Task.Delay(1000, ct);
                }

                await _processingSlot.WaitAsync(ct);
                try
                {
                    _logger.LogInformation("=== Iniciando procesamiento: {File} ===", next);
                    await ProcesarPlmxmlClaimedAsync(next, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    _logger.LogWarning("Cancelación solicitada durante procesamiento.");
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error no controlado procesando archivo CLAIMED: {File}", next);
                }
                finally
                {
                    _processingSlot.Release();
                    // Post-proceso: rescan + actualizar visibilidad
                    try { EncolarBacklog(); } catch (Exception ex) { _logger.LogError(ex, "Error durante rescan post-proceso."); }
                    ActualizarProximo();
                }
            }
        }
    }

    private async Task PeriodicRescanAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(RescanInterval, ct);
                EncolarBacklog();
                EncolarContextoBacklog();

                // Re-señalar si hay claimed pendientes y el ProcessLoop quedó sin señal
                // (defensa ante pérdida de señal por cualquier motivo)
                if (ObtenerSiguienteClaimedPath() is not null)
                    SignalQueue();

                ActualizarProximo();
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex) { _logger.LogError(ex, "Error en PeriodicRescanAsync."); }
    }

    // =========================================================================
    // CLAIM: mueve el archivo a _CLAIMED con carpeta prefijada por secuencia
    // =========================================================================

    private async Task<string?> TryClaimAsync(string rawPath, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(rawPath)) return null;

        if (!string.IsNullOrWhiteSpace(_claimedDir) &&
            rawPath.StartsWith(_claimedDir, StringComparison.OrdinalIgnoreCase))
            return null;

        if (!_claiming.TryAdd(rawPath, 0)) return null;

        try
        {
            if (!File.Exists(rawPath)) return null;

            await WaitForFileReadyAsync(rawPath, ct);

            if (!File.Exists(rawPath)) return null;

            var originalFileName = Path.GetFileName(rawPath);
            var originalName     = Path.GetFileNameWithoutExtension(rawPath);
            var ext              = Path.GetExtension(rawPath);

            // *** CAMBIO CLAVE ***
            // La carpeta de staging lleva un prefijo numérico de 6 dígitos.
            // Eso determina el orden de procesamiento (lexicográfico = numérico).
            var seq        = Interlocked.Increment(ref _secuencia);
            var jobId      = NewShortId(8);
            var jobDirName = $"{seq:D6}_{jobId}";
            var jobDir     = Path.Combine(_claimedDir, jobDirName);
            Directory.CreateDirectory(jobDir);

            var dest = Path.Combine(jobDir, originalFileName);
            if (File.Exists(dest))
                dest = Path.Combine(jobDir, $"{originalName}__{jobId}{ext}");

            File.Move(rawPath, dest);

            _logger.LogInformation("CLAIM OK [seq={Seq}, carpeta={Dir}]: {Src} -> {Dest}",
                seq, jobDirName, rawPath, dest);
            return dest;
        }
        catch (IOException ioex)
        {
            _logger.LogWarning(ioex, "No se pudo CLAIM (IO). Se reintentará vía rescan. Archivo: {File}", rawPath);
            return null;
        }
        catch (UnauthorizedAccessException uaex)
        {
            _logger.LogWarning(uaex, "No se pudo CLAIM (permisos). Archivo: {File}", rawPath);
            return null;
        }
        finally
        {
            _claiming.TryRemove(rawPath, out _);
        }
    }

    // =========================================================================
    // PROCESAMIENTO (sin cambios respecto al original)
    // =========================================================================

    private async Task ProcesarPlmxmlClaimedAsync(string claimedFilePath, CancellationToken ct)
    {
        await WaitForFileReadyAsync(claimedFilePath, ct);

        if (!File.Exists(claimedFilePath))
        {
            _logger.LogWarning("El archivo CLAIMED ya no existe al momento de procesar: {File}", claimedFilePath);
            return;
        }

        var jobDir = Path.GetDirectoryName(claimedFilePath);
        var jobId  = "";

        if (!string.IsNullOrWhiteSpace(jobDir))
            jobId = new DirectoryInfo(jobDir).Name;

        if (string.IsNullOrWhiteSpace(jobId) || string.Equals(jobId, "_CLAIMED", StringComparison.OrdinalIgnoreCase))
            jobId = NewShortId(8);

        var originalFileName  = Path.GetFileName(claimedFilePath);
        var originalBase      = Path.GetFileNameWithoutExtension(claimedFilePath);
        var baseParaCarpeta   = DerivarNombreMbomDesdeArchivo(originalBase);
        var safeBaseForFolder = SanitizarNombreParaWindows(baseParaCarpeta);
        safeBaseForFolder     = Truncar(safeBaseForFolder, 60);

        var mbomFolderName = $"M-BOM_{safeBaseForFolder}";
        var mbomFolderPath = Path.Combine(_cfg.ProcesadosRootPath, mbomFolderName);

        if (Directory.Exists(mbomFolderPath))
        {
            mbomFolderPath = Path.Combine(_cfg.ProcesadosRootPath, $"{mbomFolderName}__{jobId}");

            if (Directory.Exists(mbomFolderPath))
            {
                var suffix = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                mbomFolderPath = Path.Combine(_cfg.ProcesadosRootPath, $"{mbomFolderName}_{suffix}");
            }

            _logger.LogWarning("Carpeta destino ya existía. Se usará: {Folder}", mbomFolderPath);
        }

        var bopPendientesPath = Path.Combine(mbomFolderPath, "BOP_Pendientes");
        var bopProcesadasPath = Path.Combine(mbomFolderPath, "BOP_Procesadas");
        var mbomProcesadaPath = Path.Combine(mbomFolderPath, "MBOM_Procesada");

        Directory.CreateDirectory(mbomFolderPath);
        Directory.CreateDirectory(bopPendientesPath);
        Directory.CreateDirectory(bopProcesadasPath);
        Directory.CreateDirectory(mbomProcesadaPath);

        _logger.LogInformation("Estructura creada: {MbomFolder}", mbomFolderPath);

        var destPlmxmlPath = Path.Combine(mbomFolderPath, originalFileName);

        if (File.Exists(destPlmxmlPath))
        {
            destPlmxmlPath = Path.Combine(mbomFolderPath,
                $"{originalBase}__{jobId}{Path.GetExtension(claimedFilePath)}");

            if (File.Exists(destPlmxmlPath))
            {
                var suffix = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                destPlmxmlPath = Path.Combine(mbomFolderPath,
                    $"{originalBase}_{suffix}{Path.GetExtension(claimedFilePath)}");
            }
        }

        File.Move(claimedFilePath, destPlmxmlPath, overwrite: true);
        _logger.LogInformation("Archivo movido a: {Dest}", destPlmxmlPath);

        if (!string.IsNullOrWhiteSpace(jobDir))
            TryDeleteJobDirIfEmpty(jobDir);

        _logger.LogInformation("Ejecutando Intermedio: {Exe} {Arg1} {Arg2}",
            _cfg.ExeIntermedioPath, destPlmxmlPath, bopPendientesPath);

        var exit1 = await RunProcessAsync(
            _cfg.ExeIntermedioPath,
            new[] { destPlmxmlPath, bopPendientesPath },
            mbomFolderPath, ct);

        _logger.LogInformation("Intermedio finalizó con exitCode={ExitCode}", exit1);

        _logger.LogInformation("Ejecutando Principal: {Exe} {Arg1} {Arg2} {Arg3} {Arg4}",
            _cfg.ExePrincipalPath, mbomFolderPath, mbomProcesadaPath, bopPendientesPath, bopProcesadasPath);

        var exit2 = await RunProcessAsync(
            _cfg.ExePrincipalPath,
            new[] { mbomFolderPath, mbomProcesadaPath, bopPendientesPath, bopProcesadasPath },
            mbomFolderPath, ct);

        _logger.LogInformation("Principal finalizó con exitCode={ExitCode}", exit2);
    }

    // =========================================================================
    // UTILIDADES (sin cambios respecto al original)
    // =========================================================================

    private static string DerivarNombreMbomDesdeArchivo(string originalBase)
    {
        if (string.IsNullOrWhiteSpace(originalBase)) return "SIN_NOMBRE";
        var idx = originalBase.IndexOf("M-", StringComparison.OrdinalIgnoreCase);
        return idx >= 0 ? originalBase.Substring(idx) : originalBase;
    }

    private void TryDeleteJobDirIfEmpty(string jobDir)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(jobDir) || !Directory.Exists(jobDir)) return;
            if (!Directory.EnumerateFileSystemEntries(jobDir).Any())
                Directory.Delete(jobDir, recursive: false);
        }
        catch (Exception ex) { _logger.LogDebug(ex, "No se pudo borrar jobDir vacío: {Dir}", jobDir); }
    }

    private async Task WaitForFileReadyAsync(string path, CancellationToken ct)
    {
        var timeout    = TimeSpan.FromSeconds(_cfg.FileReadyTimeoutSeconds);
        var start      = DateTime.UtcNow;
        long lastSize  = -1;
        int stableCount = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            if (DateTime.UtcNow - start > timeout)
                throw new TimeoutException($"Timeout esperando que el archivo esté listo: {path}");

            if (!File.Exists(path))
            {
                await Task.Delay(_cfg.FileReadyPollMs, ct);
                continue;
            }

            try
            {
                var fi   = new FileInfo(path);
                var size = fi.Length;

                if (size == lastSize && size > 0) stableCount++;
                else stableCount = 0;

                lastSize = size;

                using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None);
                if (stableCount >= 2) return;
            }
            catch { /* aún en uso */ }

            await Task.Delay(_cfg.FileReadyPollMs, ct);
        }
    }

    private async Task<int> RunProcessAsync(
        string exePath, string[] args, string workingDirectory, CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName         = exePath,
            WorkingDirectory = workingDirectory,
            UseShellExecute  = false,
            CreateNoWindow   = true,
            RedirectStandardOutput = true,
            RedirectStandardError  = true
        };

        foreach (var a in args)
            psi.ArgumentList.Add(a);

        using var proc = new Process { StartInfo = psi, EnableRaisingEvents = true };

        proc.OutputDataReceived += (_, e) =>
        {
            if (!string.IsNullOrWhiteSpace(e.Data))
                _logger.LogInformation("[{Exe}] {Line}", Path.GetFileName(exePath), e.Data);
        };
        proc.ErrorDataReceived += (_, e) =>
        {
            if (!string.IsNullOrWhiteSpace(e.Data))
                _logger.LogError("[{Exe}][ERR] {Line}", Path.GetFileName(exePath), e.Data);
        };

        if (!proc.Start())
            throw new InvalidOperationException($"No se pudo iniciar el proceso: {exePath}");

        proc.BeginOutputReadLine();
        proc.BeginErrorReadLine();

        await proc.WaitForExitAsync(ct);
        return proc.ExitCode;
    }

    private static string SanitizarNombreParaWindows(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return "SIN_NOMBRE";

        var invalid = Path.GetInvalidFileNameChars();
        var sb = new System.Text.StringBuilder(input.Length);

        foreach (var ch in input)
        {
            if (char.IsWhiteSpace(ch))              { sb.Append('_'); continue; }
            if (ch == '"' || ch == '\'')              continue;
            if (ch == '/' || ch == '\\')             { sb.Append('-'); continue; }
            if (ch == '%')                            continue;
            if (ch is '&' or '|' or '<' or '>' or '^' or '!') { sb.Append('_'); continue; }
            if (invalid.Contains(ch))                { sb.Append('_'); continue; }
            sb.Append(ch);
        }

        var s = sb.ToString().Trim().TrimEnd('.');
        while (s.Contains("__")) s = s.Replace("__", "_");
        while (s.Contains("--")) s = s.Replace("--", "-");
        s = s.Replace("_-_", "-").Replace("-_", "-").Replace("_-", "-");

        if (string.IsNullOrWhiteSpace(s)) s = "SIN_NOMBRE";
        if (s.Length > 120) s = s.Substring(0, 120);

        return s;
    }

    private static string NewShortId(int len)
    {
        var s = Guid.NewGuid().ToString("N");
        return s.Substring(0, Math.Clamp(len, 4, 32));
    }

    private static string Truncar(string s, int maxLen)
    {
        if (string.IsNullOrEmpty(s)) return s;
        if (maxLen <= 0) return "";
        return s.Length <= maxLen ? s : s.Substring(0, maxLen);
    }

    private static string QuitarSufijoNumerico(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return s;
        var idx = s.LastIndexOf("__", StringComparison.Ordinal);
        if (idx < 0) return s;
        var tail = s.Substring(idx + 2);
        if (int.TryParse(tail, out _)) return s.Substring(0, idx);
        return s;
    }

    // =========================================================================
    // CONTEXTO — backlog, enqueue, loop, procesamiento
    // =========================================================================

    private void EncolarContextoBacklog()
    {
        if (string.IsNullOrWhiteSpace(_cfg.ContextoFilePrefix)) return;

        var files = Directory
            .EnumerateFiles(_cfg.MbomPendientesPath, "*.plmxml", SearchOption.TopDirectoryOnly)
            .Where(f => EsContexto(f))
            .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (files.Count > 0)
            _logger.LogInformation("Backlog Contexto: {Count} archivos .plmxml con prefijo '{Prefix}'",
                files.Count, _cfg.ContextoFilePrefix);

        foreach (var f in files)
            TryEnqueueContexto(f, "Backlog");
    }

    private void TryEnqueueContexto(string fullPath, string reason)
    {
        if (string.IsNullOrWhiteSpace(fullPath)) return;
        if (!_contextoEnqueued.TryAdd(fullPath, 0)) return;

        Interlocked.Increment(ref _contextosActivos);
        _contextoQueue.Writer.TryWrite(fullPath);
        _logger.LogInformation("Detectado Contexto ({Reason}): {File}", reason, fullPath);
    }

    private async Task ContextoLoopAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var filePath in _contextoQueue.Reader.ReadAllAsync(ct))
            {
                await _processingSlot.WaitAsync(ct);
                try
                {
                    await ProcesarContextoAsync(filePath, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    _logger.LogWarning("Cancelación solicitada en ContextoLoop.");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error no controlado procesando Contexto: {File}", filePath);
                }
                finally
                {
                    _processingSlot.Release();
                    Interlocked.Decrement(ref _contextosActivos);
                    _contextoEnqueued.TryRemove(filePath, out _);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    private async Task ProcesarContextoAsync(string pendingFilePath, CancellationToken ct)
    {
        await WaitForFileReadyAsync(pendingFilePath, ct);

        if (!File.Exists(pendingFilePath))
        {
            _logger.LogWarning("El archivo Contexto ya no existe al momento de procesar: {File}", pendingFilePath);
            return;
        }

        var originalFileName = Path.GetFileName(pendingFilePath);
        var originalBase     = Path.GetFileNameWithoutExtension(pendingFilePath);
        var safeBase         = SanitizarNombreParaWindows(originalBase);
        safeBase             = Truncar(safeBase, 60);
        var jobId            = NewShortId(8);

        var contextoFolderName = $"CONTEXTO_{safeBase}";
        var contextoFolderPath = Path.Combine(_cfg.ContextoProcesadosRootPath, contextoFolderName);

        if (Directory.Exists(contextoFolderPath))
        {
            contextoFolderPath = Path.Combine(_cfg.ContextoProcesadosRootPath, $"{contextoFolderName}__{jobId}");
            _logger.LogWarning("Carpeta Contexto ya existía. Se usará: {Folder}", contextoFolderPath);
        }

        Directory.CreateDirectory(contextoFolderPath);
        _logger.LogInformation("Estructura Contexto creada: {Folder}", contextoFolderPath);

        // Mover el .plmxml a la carpeta de trabajo
        var destFilePath = Path.Combine(contextoFolderPath, originalFileName);
        File.Move(pendingFilePath, destFilePath, overwrite: true);
        _logger.LogInformation("Archivo Contexto movido a: {Dest}", destFilePath);

        // 1) Intermedio: extrae UID y corre tcxml_export para generar CarExport.xml
        var carExportBase = Path.GetFileNameWithoutExtension(destFilePath);
        if (!string.IsNullOrWhiteSpace(_cfg.ContextoFilePrefix) &&
            carExportBase.StartsWith(_cfg.ContextoFilePrefix, StringComparison.OrdinalIgnoreCase))
            carExportBase = carExportBase.Substring(_cfg.ContextoFilePrefix.Length).TrimStart('_', ' ');
        var carExportName = carExportBase + "_export.xml";
        var carExportPath = Path.Combine(contextoFolderPath, carExportName);

        _logger.LogInformation("Ejecutando Intermedio Contexto: {Exe} {Arg1} {Arg2} {Arg3}",
            _cfg.ExeIntermedioContextoPath, destFilePath, _cfg.TcxmlExportDir, carExportPath);

        var exit1 = await RunProcessAsync(
            _cfg.ExeIntermedioContextoPath,
            new[] { destFilePath, _cfg.TcxmlExportDir, carExportPath, _cfg.TcxmlConfigBat },
            contextoFolderPath, ct);

        _logger.LogInformation("Intermedio Contexto finalizó con exitCode={ExitCode}", exit1);

        if (exit1 != 0 || !File.Exists(carExportPath))
        {
            _logger.LogError("El intermedio no generó CarExport.xml. Abortando procesamiento de Contexto.");
            return;
        }

        // 2) WebService: procesa el CarExport.xml completo y envía a Protheus
        _logger.LogInformation("Ejecutando WebService Contexto: {Exe} {Arg1}",
            _cfg.ExePrincipalContextoPath, carExportPath);

        var exit2 = await RunProcessAsync(
            _cfg.ExePrincipalContextoPath,
            new[] { carExportPath },
            contextoFolderPath, ct);

        _logger.LogInformation("WebService Contexto finalizó con exitCode={ExitCode}", exit2);
    }
}
