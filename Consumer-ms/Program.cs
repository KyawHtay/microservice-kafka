using System;
using System.Data.SqlClient;
using System.Linq;
using Confluent.Kafka;
using Dapper;

namespace Consumer_ms
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-consumer",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<string, string>(config).Build())
            {
                consumer.Subscribe("test-topic-cf");

                var connectionString = "Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=MyDatabase;Integrated Security=True;";

                while (true)
                {
                    var message = consumer.Consume();

                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();

                        var key = message.Key;
                        var value = double.Parse(message.Value);
                        var time = DateTime.UtcNow;

                        connection.Execute("INSERT INTO MyTable (key, time, value) VALUES (@key, @time, @value)", new { key, time, value });
                    }

                    Console.WriteLine($"Received message: {message}");
                }
            }
        }
    }
}
