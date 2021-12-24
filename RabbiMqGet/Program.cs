using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RabbitLink;
using RabbitLink.Consumer;
using RabbitLink.Logging;
using RabbitLink.Messaging;
using RabbitLink.Serialization;
using RabbitLink.Serialization.Json;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.SystemConsole.Themes;

namespace RabbiMqGet
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
                .Enrich.FromLogContext()
                .WriteTo.Console(theme: AnsiConsoleTheme.Literate)
                .CreateLogger();
            try
            {
                var config = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .Build();

                var rabbitMq = config["RabbitMq"];
                Log.Information("RabbitMq connection: {RabbitMq}", rabbitMq);
                using var link = LinkBuilder.Configure
                    .Uri(rabbitMq)
                    .LoggerFactory(new LinkLogFactory())
                    .Serializer(new LinkJsonSerializer())
                    .Build();

                var count = 0;
                var folder = config["OutputFolder"];
                var maxCount = ushort.Parse(config["Count"]);
                var queue = config["Queue"];
                Log.Information("Taking {Count} messages from {Queue} and put into {Folder}", maxCount, queue, folder);
                var onComplete = new TaskCompletionSource();

                async Task<LinkConsumerAckStrategy> OnMessage(ILinkConsumedMessage<JObject> msg)
                {
                    var num = Interlocked.Increment(ref count);
                    if (num >= maxCount)
                    {
                        onComplete.TrySetResult();
                        Log.Information("Last message!");
                    }

                    await File.WriteAllTextAsync(Path.Combine(folder, $"Msg{num}.json"),
                        JsonConvert.SerializeObject(msg, Formatting.Indented), Encoding.UTF8);
                    Log.Information("Write Msg{Num}.json complete.", num);
                    await onComplete.Task;
                    return LinkConsumerAckStrategy.Requeue;
                }



                using var consumer = link.Consumer
                    .AutoAck(false)
                    .PrefetchCount(maxCount)
                    .Queue(topology => topology.QueueDeclarePassive(queue))
                    .Handler<JObject>(OnMessage)
                    .Build();

                await onComplete.Task;
                Log.Information("Finished!");
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Fatal!");
            }
            finally
            {
                Log.CloseAndFlush();
            }



        }
    }

    public class LinkLogFactory : ILinkLoggerFactory
    {
        public ILinkLogger CreateLogger(string name)
        {
            return new Logger(name);
        }

        private class Logger : ILinkLogger
        {
            private readonly string _category;

            public Logger(string category)
            {
                _category = category;
            }

            public void Dispose()
            {

            }

            public void Write(LinkLoggerLevel level, string message)
            {
                switch (level)
                {
                    case LinkLoggerLevel.Error:
                        Log.Error("{Category}: {Event}", _category, message);
                        break;
                    case LinkLoggerLevel.Warning:
                        Log.Warning("{Category}: {Event}", _category, message);
                        break;
                    case LinkLoggerLevel.Info:
                        Log.Information("{Category}: {Event}", _category, message);
                        break;
                    case LinkLoggerLevel.Debug:
                        Log.Debug("{Category}: {Event}", _category, message);
                        break;
                    default:
                        //throw new ArgumentOutOfRangeException(nameof(level), level, null);
                        break;
                }
            }
        }
    }
}


