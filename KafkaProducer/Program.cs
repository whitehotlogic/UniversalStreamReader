using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka.Serialization;
using Confluent.Kafka;
using System.IO;

namespace KafkaProducer
{
    class Program
    {
        public static void Main(string[] args)
        {
            const string BOOTSTRAP_SERVERS = "192.168.1.118:9092";  // args[0];
            const string topicName = "testaroo"; // args[1];

            var config = new Dictionary<string, object> { { "bootstrap.servers", BOOTSTRAP_SERVERS } };


            using (var producer = new Producer<string, string>(config, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {producer.Name} producing on topic {topicName}.");
                Console.WriteLine("-----------------------------------------------------------------------");
                Console.WriteLine("To create a kafka message with UTF-8 encoded key/value message:");
                Console.WriteLine("> key value<Enter>");
                Console.WriteLine("To create a kafka message with empty key and UTF-8 encoded value:");
                Console.WriteLine("> value<enter>");
                Console.WriteLine("Ctrl-C to quit.\n");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    Console.Write("> ");

                    string text;
                    try
                    {
                        text = Console.ReadLine();
                    }
                    catch (IOException)
                    {
                        // IO exception is thrown when ConsoleCancelEventArgs.Cancel == true.
                        break;
                    }
                    if (text == null)
                    {
                        // Console returned null before 
                        // the CancelKeyPress was treated
                        break;
                    }

                    var key = "";
                    var val = text;

                    // split line if both key and value specified.
                    int index = text.IndexOf(" ");
                    if (index != -1)
                    {
                        key = text.Substring(0, index);
                        val = text.Substring(index + 1);
                    }

                    var deliveryReport = producer.ProduceAsync(topicName, key, val);
                    var result = deliveryReport.Result; // synchronously waits for message to be produced.
                    Console.WriteLine($"Partition: {result.Partition}, Offset: {result.Offset}");
                }
            }
        }
    }
}
