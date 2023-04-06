using System;
using Confluent.Kafka;

namespace microservice_kafka
{

    class Program
    {
        static void Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = "test-producer"
            };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                var rnd = new Random();

                while (true)
                {
                    var key = rnd.Next(1, 3);
                    var value = rnd.NextDouble().ToString();
                    var message = new Message<string, string> { Key = key.ToString(), Value = value };
                    producer.Produce("test-topic-cf", message);
                    Console.WriteLine($"Sent message: {message}");
                    System.Threading.Thread.Sleep(500);
                }
            }
        }
    }
}
