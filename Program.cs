using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;

namespace RabbitMqJsonSender
{
    class Program
    {

        private static IConfiguration LoadConfiguration()
        {
            return new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("config.json")
                .Build();
        }

        static async Task Main(string[] args)
        {
            var configuration = LoadConfiguration();

            string directory = configuration[key: "JsonDirectory"];
            string nomeFila = configuration[key: "QueueName"];

            if (!Directory.Exists(directory))
            {
                Console.WriteLine($"Diretorio nao encontrado: {directory}");
                return;
            }

            var factory = new ConnectionFactory() { HostName = "localhost" };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.QueueDeclare(queue: nomeFila,
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
                            SendMessage(channel, element.GetRawText(), nomeFila);
                        }
                    }
                    else
                    {
                        SendMessage(channel, jsonDocument.RootElement.GetRawText(), nomeFila);
                    }
                }
                catch (JsonException ex)
                {
                    Console.WriteLine($"Falha ao fazer o parse do arquivo {filePath}: {ex.Message}");
                }
            }
        }



        private static void SendMessage(IModel channel, string message, string nomeFila)
        {
            var body = Encoding.UTF8.GetBytes(message);
            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;

            channel.BasicPublish(exchange: "",
                                 routingKey: nomeFila,
                                 basicProperties: properties,
                                 body: body);

            Console.WriteLine($"[X]JSON ENVIADO PARA FILA:\n {message}");
        }
    }
}