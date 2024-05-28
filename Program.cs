using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;

namespace RabbitMqJsonSender
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configuration = LoadConfiguration();

            // Obtém o caminho do diretório dos arquivos JSON do arquivo de configuração
            string directory = configuration[key: "JsonDirectory"];

            if (!Directory.Exists(directory))
            {
                Console.WriteLine($"Directory not found: {directory}");
                return;
            }

            var factory = new ConnectionFactory() { HostName = "localhost" };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.QueueDeclare(queue: "json_queue",
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            foreach (var filePath in Directory.GetFiles(directory, "*.json"))
            {
                string jsonString = await File.ReadAllTextAsync(filePath);
                try
                {
                    var jsonDocument = JsonDocument.Parse(jsonString);

                    if (jsonDocument.RootElement.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var element in jsonDocument.RootElement.EnumerateArray())
                        {
                            SendMessage(channel, element.GetRawText());
                        }
                    }
                    else
                    {
                        SendMessage(channel, jsonDocument.RootElement.GetRawText());
                    }
                }
                catch (JsonException ex)
                {
                    Console.WriteLine($"Failed to parse JSON from file {filePath}: {ex.Message}");
                }
            }
        }

        static IConfiguration LoadConfiguration()
        {
            return new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("config.json")
                .Build();
        }

        static void SendMessage(IModel channel, string message)
        {
            var body = Encoding.UTF8.GetBytes(message);
            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;

            channel.BasicPublish(exchange: "",
                                 routingKey: "json_queue",
                                 basicProperties: properties,
                                 body: body);

            Console.WriteLine($"Sent message: {message}");
        }
    }
}