using DescarConector.WindowsService;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

IHost host = Host.CreateDefaultBuilder(args)
    .UseWindowsService(options =>
    {
        options.ServiceName = "Descar Conector MBOM Service";
    })
    .ConfigureAppConfiguration((context, config) =>
    {
        config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
    })
    .ConfigureLogging((context, logging) =>
    {
        logging.ClearProviders();
        logging.AddConsole();

        var settings = context.Configuration.GetSection("Conector").Get<ConectorSettings>() ?? new ConectorSettings();
        if (!string.IsNullOrWhiteSpace(settings.LogPath))
            logging.AddProvider(new FileLoggerProvider(settings.LogPath));
    })
    .ConfigureServices((context, services) =>
    {
        services.Configure<ConectorSettings>(context.Configuration.GetSection("Conector"));
        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();
