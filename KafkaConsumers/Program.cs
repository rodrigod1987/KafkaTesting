using Confluent.Kafka;
using Serilog;
using System.Threading;
using System;

namespace KafkaConsumers
{
    public class Program
    {
        static void Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            logger.Information("Testando o consumo de mensagens com Kafka");

            if (args.Length != 2)
            {
                logger.Error(
                    "Informe 2 parâmetros: " +
                    "no primeiro o IP/porta para testes com o Kafka, " +
                    "no segundo o Topic a ser utilizado no consumo das mensagens...");

                return;
            }

            string bootstrapServers = args[0];
            string topicName = args[1];

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {topicName}");

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"{topicName}-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            CancellationTokenSource cts = new();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
                consumer.Subscribe(topicName);

                try
                {
                    while (true)
                    {
                        var cr = consumer.Consume(cts.Token);
                        logger.Information(
                            $"Mensagem lida: {cr.Message.Value}");
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                    logger.Warning("Cancelada a execução do Consumer...");
                }
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
    }
}