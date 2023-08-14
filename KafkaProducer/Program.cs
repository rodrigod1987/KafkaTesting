using Confluent.Kafka;
using Serilog;
using System;
using System.Threading.Tasks;

namespace KafkaProducer
{
    public class Program
    {
        static async Task  Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            logger.Information("Testando o envio de mensagens com Kafka");

            if (args.Length < 3)
            {
                logger.Error(
                    "Informe ao menos 3 parâmetros: " +
                    "no primeiro o IP/porta para testes com o Kafka, " +
                    "no segundo o Topic que receberá a mensagem, " +
                    "já no terceito em diante as mensagens a serem " +
                    "enviadas a um Topic no Kafka...");
                return;
            }

            string bootstrapServers = args[0];
            string topicName = args[1];

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {topicName}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 2; i < args.Length; i++)
                    {
                        var result = await producer.ProduceAsync(
                            topicName,
                            new Message<Null, string>
                            { Value = args[i] });

                        logger.Information(
                            $"Mensagem: {args[i]} | " +
                            $"Status: {result.Status}");
                    }
                }

                logger.Information("Concluído o envio de mensagens");
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
    }
}