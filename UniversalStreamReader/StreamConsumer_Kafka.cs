using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Text;

namespace UniversalStreamReader
{
    public class StreamConsumer_Kafka : IStreamConsumer
    {

        private ICache imc;
        private IPersist[] ip;


        public StreamConsumer_Kafka(ICache imc, IPersist[] ip)
        {
            this.imc = imc;
            this.ip = ip;
        }


        private static Dictionary<string, object> constructConfig(string brokerList, bool enableAutoCommit) =>
                                                                                new Dictionary<string, object>
        {
            { "group.id", "UniversalStreamReader" }, // change the group.id to get all messages for the topic from beginning of time
            { "enable.auto.commit", enableAutoCommit },
            { "auto.commit.interval.ms", 5000 },
            //{ "statistics.interval.ms", 60000 }, // statistics event timer, for debugging
            { "bootstrap.servers", brokerList },
            { "default.topic.config", new Dictionary<string, object>()
                {
                    { "auto.offset.reset", "smallest" }
                }
            }
        };

        //TODO: dependency inject consumer so that Run_Poll can be tested without communication with kafka
        public void Run_Poll(string brokerList, List<string> topics)
        {

            using (var consumer = new Consumer<string, string>(constructConfig(brokerList, true), new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.OnMessage += (_, msg) => // new message found for subscribed topic
                {
                    Console.WriteLine($"NEW_MESSAGE: Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Key} {msg.Value}");

                    long created = (long)(DateTime.Now.Ticks);  // "created" is just a numeric value based on time, 
                                                                // jic i need to sort at any point outside of kafka values

                    imc.addMessage(msg.Topic, msg.Key, msg.Value, created); //write to in-memory cache

                    foreach (IPersist persistentStorageImplementation in ip) //perform each storage implementation for this message
                        persistentStorageImplementation.Persist(msg.Topic, msg.Key, msg.Value, created);
                };

                consumer.OnPartitionEOF += (_, end)
                    => Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

                consumer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error}");

                consumer.OnConsumeError += (_, msg)
                    => Console.WriteLine($"Error consuming from topic/partition/offset {msg.Topic}/{msg.Partition}/{msg.Offset}: {msg.Error}");

                consumer.OnOffsetsCommitted += (_, commit) =>
                {
                    if (commit.Error)
                    {
                        Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                    }
                    Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]"); // some offsets might still be committed
                };

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                    consumer.Assign(partitions);
                };

                consumer.OnPartitionsRevoked += (_, partitions) =>
                {
                    Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    consumer.Unassign();
                };

                // output periodic statistics for debug purposes (configured using statistics.interval.ms above)
                consumer.OnStatistics += (_, json)
                    => Console.WriteLine($"DEBUG: Statistics: {json}");

                consumer.Subscribe(topics);

                Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");


                bool cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                Console.WriteLine("Ctrl-C to exit.");
                while (!cancelled)
                {
                    //Console.WriteLine("DEBUG: Client is polling");
                    consumer.Poll(TimeSpan.FromMilliseconds(100));

                }
            }
        }
    }
}
