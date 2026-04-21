using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace DescarConector.WindowsService;

public sealed class FileLoggerProvider : ILoggerProvider
{
    private readonly string _logFilePath;
    private readonly object _lock = new();

    public FileLoggerProvider(string logFilePath)
    {
        _logFilePath = logFilePath;
        var dir = Path.GetDirectoryName(_logFilePath);
        if (!string.IsNullOrWhiteSpace(dir))
            Directory.CreateDirectory(dir);
    }

    public ILogger CreateLogger(string categoryName) => new FileLogger(categoryName, _logFilePath, _lock);

    public void Dispose() { }
}

internal sealed class FileLogger : ILogger
{
    private readonly string _category;
    private readonly string _path;
    private readonly object _lock;

    public FileLogger(string category, string path, object sharedLock)
    {
        _category = category;
        _path = path;
        _lock = sharedLock;
    }

    public IDisposable BeginScope<TState>(TState state) where TState : notnull
    => NullScope.Instance;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        var line =
            $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{logLevel}] {_category} - {formatter(state, exception)}" +
            (exception is null ? "" : $" | EX: {exception}");

        lock (_lock)
        {
            File.AppendAllText(_path, line + Environment.NewLine);
        }
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}

