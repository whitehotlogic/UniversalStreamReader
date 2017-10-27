using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
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

            const string DATABASE_FILE = @"c:\tempDev\AllData.sqlite";
            const string BOOTSTRAP_SERVERS = "192.168.1.118:9092";
            List<string> topicsToConsume = new List<string>();
            topicsToConsume.Add("testaroo");
            //topicsToConsume.Add("dummytopic1");
            //topicsToConsume.Add("dummytopic5");
            //topicsToConsume.Add("dummytopic6");


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
     * 
     *    Kafka Consumer
     * 
     * 
     * 
     * 
     */

    class StreamConsumer_Kafka {

        const string PERSISTENCE_FILE = @"c:\tempDev\CacheData.txt";
        const int RINGBUFFER_SIZE = 10;

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


        public static void Run_Poll(string brokerList, List<string> topics)
        {

            //initialize in-memory cache
            InMemoryCacheWithFilePersist imcwfp = new InMemoryCacheWithFilePersist(RINGBUFFER_SIZE, PERSISTENCE_FILE);

            //using (var consumer = new Consumer<Null, string>(constructConfig(brokerList, true), IDeserializer<string>, new StringDeserializer(Encoding.UTF8)))
            using (var consumer = new Consumer<string, string>(constructConfig(brokerList, true), new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                // Note: All event handlers are called on the main thread.

                // NEW MESSAGE FOUND FOR TOPIC!
                consumer.OnMessage += (_, msg) =>
                {

                    
                    Console.WriteLine($"NEW_MESSAGE: Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");


                    //write to in-memory cache
                    imcwfp.addMessage(msg.Topic, msg.Value, DateTime.UtcNow);

                    //write to DB

                };

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


    class InMemoryCacheWithFilePersist
    {





        //ConcurrentDictionary<string,List<KafkaMessage>> cache = null;
        //ConcurrentDictionary<string,Dictionary<DateTime,string>> cache = null; // use ConcurrentDictionary because it's thread-safe with atomic ops, and O(1)
        //SortedDictionary<string,List<string,string>> cache = null;
        string[,] stringArrayCache = null;


        private String persistenceFilePath = null;

        private bool cacheIsFull = false;
        private int indexToWrite = 0;
        private int ringBufferSize;

        public InMemoryCacheWithFilePersist(int ringBufferSize, string persistenceFilePath)
        {

            //this.cache = new ConcurrentDictionary<string,Dictionary<DateTime,string>>(); //for initial testing, use strings for key/value
            //this.cache = new ConcurrentDictionary<string,List<KafkaMessage>>();
            //this.cache = new SortedDictionary<string,string>();

            this.ringBufferSize = ringBufferSize;
            stringArrayCache = new string[ringBufferSize, 3]; // number of rows in array predefined by ringBufferSize
                                                              // three strings are created, topic, message
            

            this.persistenceFilePath = persistenceFilePath;


            try // check and see if cache persistence file exists, and if not, create it
            {
                if (!File.Exists(persistenceFilePath))
                {
                    File.Create(persistenceFilePath).Dispose();
                }
            } catch (IOException e)
            {
                Console.WriteLine("ERROR: Could not create cache persistence file -- " + e.Message);
            }

            try // will get FileNotFoundException if no cache available on disk
            {
                using (FileStream fileStream = new FileStream(persistenceFilePath, FileMode.Open))
                {

                    //assumes cache size in file matches cache size specified in RINGBUFFER_SIZE


                    //this.cache = (ConcurrentDictionary<string,List<KafkaMessage>>)bf.Deserialize(fileStream);
                    //this.cache = (ConcurrentDictionary<string,Dictionary<DateTime,string>>)bf.Deserialize(fileStream);
                    //this.cache = (OrderedDictionary)bf.Deserialize(fileStream); 
                    //this.cache = (SortedDictionary<DateTime,string>) bf.Deserialize(fileStream);


                    IFormatter bf = new BinaryFormatter();
                    this.stringArrayCache = (string[,]) bf.Deserialize(fileStream); 
                    fileStream.Close();

                    // sort by DateTime.UtcNow, so the oldest cache entry is at the beginning, and GO
                    this.stringArrayCache = stringArrayCache.OrderBy(row => row[0]); // row[0] is the created (datetime.utcnow)


                }
            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("WARNING: Persistence File Not Found  -- " + e.Message);
            }
            catch (SerializationException e)
            {
                // if empty, just display warning
                // if corrupt, display ERROR and quit program waiting 10secs for user to view error
                Console.WriteLine("WARNING: Persistence File Empty or Corrupt  -- " + e.Message);
            }
            

            

            Console.WriteLine("INFO: In-Memory Cache Initialized with " + ringBufferSize + " records");

        }

        public void addMessage(string topic, string message, DateTime created)
        {

            if (indexToWrite == ringBufferSize - 1) // if we've reached the end of the ringbuffer / cache
                indexToWrite = 0;

            this.stringArrayCache[indexToWrite, 0] = created.Ticks.ToString(); // gives me a sortable  column
            this.stringArrayCache[indexToWrite, 1] = topic;
            this.stringArrayCache[indexToWrite, 2] = message;

            indexToWrite++; // set pointer to next index in the cache for next message write (seems ringbuffery to me)

            Console.WriteLine("INFO: topic: \"" + topic + "\", with message: \"" + message + "\" has been added to cache at \"" + created.ToString() + "\"");

            serializedFilePersist();

            Console.WriteLine("INFO: Cache persisted to disk file");

            
        }

        public void serializedFilePersist()
        {
            using (FileStream fileStream = new FileStream(persistenceFilePath, FileMode.Create))
            {
                IFormatter bf = new BinaryFormatter();
                bf.Serialize(fileStream, this.stringArrayCache);
                fileStream.Close();
            }

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

        /*
        public int size()
        {
            //return this.cache.Count; // needed for ringbuffer byCount option? nope, this is wrong ---->
                                              // ----> need to ringbuffer by message count per topic, not topic (key) count 
        }
        */

        /*
        public Dictionary<DateTime, string> getMessagesFromCacheForTopic(string topic)
        {
            //if (this.cache.ContainsKey(topic))
            if (this.cache.Contains(topic))
            {
                return cache[topic] as Dictionary<DateTime, string>; // return all messages for this topic
            } else
            {
                return new Dictionary<DateTime, string>(); // returning empty for testing only
            }
            
        }
        */

        /*
        public void ringBufferPolicer (int policyType, int expiryValue)
        {
            //policyType==count, expiryValue==3      (remove oldest message if messagecount is > 100) 
            //policyType==time, expiryValue==600_000   (10 minutes in milliseconds)

            while (true)
            {

                //Console.WriteLine("Loop of Background Thread");
                //for(each topic in cache, get messages count)
                foreach(KeyValuePair<string,Dictionary<DateTime,string>> kvp in this.cache)
                {
                    
                    string topic = kvp.Key;

                    //Console.WriteLine(topic);
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

                        Console.WriteLine("INFO: RingbuffPolicer found and removed " + expiredCount + " expired messages for topic: " + topic);

                    }

                }
            }
     


        }
        */





    }

    public static class MultiDimensionalArrayExtensions
    {
        /// <summary>
        ///   Orders the two dimensional array by the provided key in the key selector.
        /// </summary>
        /// <typeparam name="T">The type of the source two-dimensional array.</typeparam>
        /// <param name="source">The source two-dimensional array.</param>
        /// <param name="keySelector">The selector to retrieve the column to sort on.</param>
        /// <returns>A new two dimensional array sorted on the key.</returns>
        public static T[,] OrderBy<T>(this T[,] source, Func<T[], T> keySelector)
        {
            return source.ConvertToSingleDimension().OrderBy(keySelector).ConvertToMultiDimensional();
        }
        /// <summary>
        ///   Orders the two dimensional array by the provided key in the key selector in descending order.
        /// </summary>
        /// <typeparam name="T">The type of the source two-dimensional array.</typeparam>
        /// <param name="source">The source two-dimensional array.</param>
        /// <param name="keySelector">The selector to retrieve the column to sort on.</param>
        /// <returns>A new two dimensional array sorted on the key.</returns>
        public static T[,] OrderByDescending<T>(this T[,] source, Func<T[], T> keySelector)
        {
            return source.ConvertToSingleDimension().
                OrderByDescending(keySelector).ConvertToMultiDimensional();
        }
        /// <summary>
        ///   Converts a two dimensional array to single dimensional array.
        /// </summary>
        /// <typeparam name="T">The type of the two dimensional array.</typeparam>
        /// <param name="source">The source two dimensional array.</param>
        /// <returns>The repackaged two dimensional array as a single dimension based on rows.</returns>
        private static IEnumerable<T[]> ConvertToSingleDimension<T>(this T[,] source)
        {
            T[] arRow;

            for (int row = 0; row < source.GetLength(0); ++row)
            {
                arRow = new T[source.GetLength(1)];

                for (int col = 0; col < source.GetLength(1); ++col)
                    arRow[col] = source[row, col];

                yield return arRow;
            }
        }
        /// <summary>
        ///   Converts a collection of rows from a two dimensional array back into a two dimensional array.
        /// </summary>
        /// <typeparam name="T">The type of the two dimensional array.</typeparam>
        /// <param name="source">The source collection of rows to convert.</param>
        /// <returns>The two dimensional array.</returns>
        private static T[,] ConvertToMultiDimensional<T>(this IEnumerable<T[]> source)
        {
            T[,] twoDimensional;
            T[][] arrayOfArray;
            int numberofColumns;

            arrayOfArray = source.ToArray();
            numberofColumns = (arrayOfArray.Length > 0) ? arrayOfArray[0].Length : 0;
            twoDimensional = new T[arrayOfArray.Length, numberofColumns];

            for (int row = 0; row < arrayOfArray.GetLength(0); ++row)
                for (int col = 0; col < numberofColumns; ++col)
                    twoDimensional[row, col] = arrayOfArray[row][col];

            return twoDimensional;
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

}
