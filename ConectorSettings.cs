using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DescarConector.WindowsService;

public sealed class ConectorSettings
{
    // MBOM
    public string MbomPendientesPath { get; set; } = "";
    public string ProcesadosRootPath { get; set; } = "";
    public string ExeIntermedioPath { get; set; } = "";
    public string ExePrincipalPath { get; set; } = "";

    // Contextos — misma carpeta de entrada que MBOM (tctemp), se distinguen por prefijo del nombre
    public string ContextoFilePrefix { get; set; } = "";          // ej: "Export_CC_Protheus"
    public string ContextoProcesadosRootPath { get; set; } = "";
    public string ExeIntermedioContextoPath { get; set; } = "";
    public string TcxmlExportDir { get; set; } = "";
    public string TcxmlConfigBat { get; set; } = "";
    public string ExePrincipalContextoPath { get; set; } = "";

    public string LogPath { get; set; } = "";
    public int FileReadyTimeoutSeconds { get; set; } = 600;
    public int FileReadyPollMs { get; set; } = 500;
}

