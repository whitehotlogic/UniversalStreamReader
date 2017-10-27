using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

// for the ring buffer -- am i doing this wrong by using threads? do i even fully understand a ring buffer? look into it further.
using System.Threading.Tasks;
using System.Linq;

// for the kafka consumer (using confluent c# wrapper for librdkafka C client)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka.Serialization;
using Confluent.Kafka;

namespace UniversalStreamReader
{
    class Program
    {
        static void Main(string[] args)
        {

            const string PERSISTENCE_FILE = @"c:\tempDev\InMemoryCacheWithPersist.txt";
            const string BOOTSTRAP_SERVERS = "192.168.1.118:9092";
            List<string> topicsToConsume = new List<string>();
            topicsToConsume.Add("testaroo");
            //topicsToConsume.Add("dummytopic1");
            //topicsToConsume.Add("dummytopic5");
            //topicsToConsume.Add("dummytopic6");


            //initialize in-memory cache
            InMemoryCacheWithPersist imcwp = new InMemoryCacheWithPersist(PERSISTENCE_FILE);


            //create background thread to manage the cache's ringbuffer policy
            Task.Factory.StartNew(() => imcwp.ringBufferPolicer(1, 3));


            //create background thread to consume kafka cluster
            Task.Factory.StartNew(() => StreamConsumer_Kafka.Run_Poll(BOOTSTRAP_SERVERS, topicsToConsume));
           
            while (true)
            {
                // keep the main thread open for testing
                Task.Delay(TimeSpan.FromMilliseconds(1000)).Wait();
            }


            /*
            // add some dummy topics and messages to the in-memorycache for testing concurrency
            int i = 0;
            while (true)
            {

                string topic = "dummytopic" + i; // create hash for topic
                for (int j = 0; j < 20; j++) { // add 20 dummy messages to each topic
                    string message = "dummymessage" + j;
                    imcwp.add(topic, message, DateTime.Now);
                }
                
                //Task.Delay(TimeSpan.FromSeconds(0.1)).Wait();

                //on close, persist to disk
                imcwp.persist();

                i++;
            }
            */

            Task.Delay(TimeSpan.FromSeconds(5)).Wait(); // so i can see the output for now

        }
    }


    /**
     * 
     * 
     * 
     *    Topic, Message Objects, for ORM
     * 
     * 
     * 
     * 
     */

    class KafkaMessage
    {
        public int Id { get; set; }
        public string Value { get; set; }
        public DateTime Created { get; set; }
    }

    class KafkaTopic
    {
        public int Id { get; set; }
        public string Value { get; set; }
    }

    /**
     * 
     * 
     * 
     * 
     *    Kafka Consumer
     * 
     * 
     * 
     * 
     */

    class StreamConsumer_Kafka { 

        private static Dictionary<string, object> constructConfig(string brokerList, bool enableAutoCommit) =>
                                                                                new Dictionary<string, object>
        {
                    { "group.id", "sd34243asd" }, // change the group.id to get all messages for the topic from beginning of time
                    { "enable.auto.commit", enableAutoCommit },
                    { "auto.commit.interval.ms", 5000 },
                    { "statistics.interval.ms", 60000 }, // why am i doing this?
                    { "bootstrap.servers", brokerList },
                    { "default.topic.config", new Dictionary<string, object>()
                        {
                            { "auto.offset.reset", "smallest" }
                        }
                    }
        };

        /// <summary>
        //      In this example:
        ///         - offsets are auto commited.
        ///         - consumer.Poll / OnMessage is used to consume messages.
        ///         - no extra thread is created for the Poll loop.
        /// </summary>
        public static void Run_Poll(string brokerList, List<string> topics)
        {
            //using (var consumer = new Consumer<Null, string>(constructConfig(brokerList, true), IDeserializer<string>, new StringDeserializer(Encoding.UTF8)))
            using (var consumer = new Consumer<string, string>(constructConfig(brokerList, true), new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                // Note: All event handlers are called on the main thread.

                consumer.OnMessage += (_, msg)
                    => Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");

                consumer.OnPartitionEOF += (_, end)
                    => Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

                consumer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error}");

                consumer.OnConsumeError += (_, msg)
                    => Console.WriteLine($"Error consuming from topic/partition/offset {msg.Topic}/{msg.Partition}/{msg.Offset}: {msg.Error}");

                consumer.OnOffsetsCommitted += (_, commit) =>
                {
                    Console.WriteLine($"[{string.Join(", ", commit.Offsets)}]");

                    if (commit.Error)
                    {
                        Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                    }
                    Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
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

                // output periodic statistics for debug purposes
                //consumer.OnStatistics += (_, json)
                //    => Console.WriteLine($"Statistics: {json}");

                consumer.Subscribe(topics);

                Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");

               

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                Console.WriteLine("Ctrl-C to exit.");
                while (!cancelled)
                {
                    //Console.WriteLine("polling");
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                    
                    
                    /* Java way:
                     * for (ConsumerRecord<String, String> record : records) // get all the messages and log them to console
                     * System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                     * 
                     * */
                }
            }
        }
    }

    /**
     * 
     * 
     * 
     * 
     *   Trivial In-Memory Cache Implementation
     *   
     *   
     *   
     *   
     */


    class InMemoryCacheWithPersist
    {
        //ConcurrentDictionary<string,List<KafkaMessage>> cache = null;
        ConcurrentDictionary<string,Dictionary<DateTime,string>> cache = null; // use ConcurrentDictionary because it's thread-safe with atomic ops, and O(1)
        private String persistenceFilePath = null;

        public InMemoryCacheWithPersist(string persistenceFilePath)
        {

            this.cache = new ConcurrentDictionary<string,Dictionary<DateTime,string>>(); //for initial testing, use strings for key/value
            //this.cache = new ConcurrentDictionary<string,List<KafkaMessage>>();
            this.persistenceFilePath = persistenceFilePath;

            try // will get FileNotFoundException if no cache available on disk
            {
                using (FileStream fileStream = new FileStream(persistenceFilePath, FileMode.Open))
                {
                    IFormatter bf = new BinaryFormatter();
                    //this.cache = (ConcurrentDictionary<string,List<KafkaMessage>>)bf.Deserialize(fileStream);
                    this.cache = (ConcurrentDictionary<string,Dictionary<DateTime,string>>)bf.Deserialize(fileStream);
                    fileStream.Close();
                }
            }
            catch (FileNotFoundException e)
            {
                Console.Out.WriteLine("Warning: Persistence File Not Found  -- " + e.Message);
            }
            catch (SerializationException e)
            {
                Console.Out.WriteLine("Warning: Persistence File Empty or Corrupt  -- " + e.Message);
            }
            
            Console.Out.WriteLine("INFO: In-Memory Cache Initialized with XXXX topics and XXXX messages");

        }


        public int size()
        {
            return this.cache.Count; // needed for ringbuffer byCount option? nope, this is wrong ---->
                                              // ----> need to ringbuffer by message count per topic, not topic (key) count 
        }


        public Dictionary<DateTime, string> get(string topic)
        {
            if (this.cache.ContainsKey(topic))
            {
                return cache[topic] as Dictionary<DateTime, string>; // return all messages for this topic
            } else
            {
                return new Dictionary<DateTime, string>(); // returning empty for testing only
            }
            
        }


        public void add(string topic, string message, DateTime created)
        {


            Dictionary<DateTime,string> messages = this.get(topic); //get any existing messages in the cache for this topic
            if (!messages.ContainsKey(created)) // make sure the same message isn't already there
            {
                messages.Add(created, message); //add the new message to the cache for this topic
            }

            this.cache[topic] = messages;

            Console.Out.WriteLine("INFO: Message \"" + message + "\" added to topic \"" + topic + "\"");
            /*
            if (!this.cache.ContainsKey(topic))
            {
                this.cache.TryAdd(topic, messages); //should be using AddOrUpdate() of course, this is just for initial testing
            }
            */

            // todo:
            //this.cache.AddOrUpdate();
            // if key/topic already exists, then update the messages
        }


        public void remove(string key, string value)
        {

        }


        public void persist()
        {
            //used a persist() method as a wrapper for invoking different persistence types 
            //    -- did this for the extensibility factor 
            //    -- assuming all persistent locations are synchronized

            serializedFilePersist();

            sqliteDatabasePersist();

        }


        public void sqliteDatabasePersist()
        {

            // todo: use ORM wrapper like Dapper for query sanitization + better readability

            // pseudocoded for the assumptions:
            //   1) that the database should update one message at a time, to stay in sync with the cache
            //   2) that the database may hold older records than the cache holds
            //   otherwise, if db only needs persisted on close, then just rewrite the entire db on close
            //     for simplicity and readability? that sounds horrible. lol.

            // create db if it doesn't already exists
            // create topic table if it doesn't already exist
            // create message table if it doesn't already exist

            // for each topic in cache, query db topic table to see if it already exists, if not insert it
                // for each message in the cache topic
                    // if latest message in cache is not the latest message in db, then just insert this new one
                    // else, check each message for existence in the db and insert if it doesn't

        }

        public void serializedFilePersist()
        {
            using (FileStream fileStream = new FileStream(persistenceFilePath, FileMode.Create))
            {
                IFormatter bf = new BinaryFormatter();
                bf.Serialize(fileStream, this.cache);
                fileStream.Close();
            }

        }


        public void ringBufferPolicer (int policyType, int expiryValue)
        {
            //policyType==count, expiryValue==3      (remove oldest message if messagecount is > 100) 
            //policyType==time, expiryValue==600_000   (10 minutes in milliseconds)

            while (true)
            {

                //Console.Out.WriteLine("Loop of Background Thread");
                //for(each topic in cache, get messages count)
                foreach(KeyValuePair<string,Dictionary<DateTime,string>> kvp in this.cache)
                {
                    
                    string topic = kvp.Key;

                    //Console.Out.WriteLine(topic);
                    //Task.Delay(TimeSpan.FromSeconds(1)).Wait();

                    Dictionary<DateTime, string> messages = kvp.Value; // just for ease of readability

                    if (messages.Count > expiryValue) // then expire the messages, (assuming count for current testing purposes)
                    {

                        int expiredCount = messages.Count - expiryValue; // get the number of messages to remove

                        //sort the dictionary, take the top X values
                        messages = messages.OrderBy(pair => pair.Key).Take(expiryValue)
                                                                     .ToDictionary(pair => pair.Key, pair => pair.Value);

                        //remove the expired values by reassigning the trimmed dictionary to the cache 
                        this.cache[topic] = messages;

                        Console.Out.WriteLine("INFO: RingbuffPolicer found and removed " + expiredCount + " expired messages for topic: " + topic);

                    }

                }
            }


        }
    }

}
