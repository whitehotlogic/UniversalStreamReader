using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;
using System.Linq;

// for the kafka consumer (using confluent c# wrapper for librdkafka C client)
using System.Text;
using Confluent.Kafka.Serialization;
using Confluent.Kafka;

namespace UniversalStreamReader
{
    class Program
    {

        // todo: is the cache appending to itself? because it shouldn't
        // cache should read from disk, then wipe the file?


        static void Main(string[] args)
        {

            const string DATABASE_FILE = @"c:\tempDev\AllData.sqlite";
            const string STREAM_SERVERS = "192.168.1.118:9092";
            List<string> topicsToConsume = new List<string>();
            topicsToConsume.Add("testaroo");
            //topicsToConsume.Add("dummytopic1");
            //topicsToConsume.Add("dummytopic5");
            //topicsToConsume.Add("dummytopic6");

            //create background thread to consume kafka cluster
            Task.Factory.StartNew(() => StreamConsumer_Kafka.Run_Poll(STREAM_SERVERS, topicsToConsume));
           
            while (true)
            {
                // keep the main thread open for testing
                Task.Delay(TimeSpan.FromMilliseconds(1000)).Wait();
            }


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
        const int RINGBUFFER_SIZE = 5;

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
                //    => Console.WriteLine($"DEBUG: Statistics: {json}");

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
                    //Console.WriteLine("DEBUG: polling");
                    consumer.Poll(TimeSpan.FromMilliseconds(100));

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

        string[,] stringArrayCache = null;

        private String persistenceFilePath = null;

        private bool cacheIsFull = false;
        private int indexToWrite = 0;
        private int ringBufferSize;

        public InMemoryCacheWithFilePersist(int ringBufferSize, string persistenceFilePath)
        {

            this.ringBufferSize = ringBufferSize;
            stringArrayCache = new string[ringBufferSize, 3]; // number of rows in array predefined by ringBufferSize
                                                              // three strings are created, topic, message
            

            this.persistenceFilePath = persistenceFilePath; // we need the value for other methods


            try // check and see if cache persistence file exists, and if not, create it
            {
                if (!File.Exists(persistenceFilePath))
                {
                    File.Create(persistenceFilePath).Dispose(); // create the cache persist file on disk
                }
            } catch (IOException e)
            {
                Console.WriteLine("ERROR: Could not create cache persistence file -- " + e.Message);
            }

            try // try to load the persistent cache file into memory
            {
                using (FileStream fileStream = new FileStream(persistenceFilePath, FileMode.Open))
                {
                    IFormatter bf = new BinaryFormatter();
                    this.stringArrayCache = (string[,]) bf.Deserialize(fileStream);  //assumes cache size in file matches cache 
                                                                                     //  size specified in RINGBUFFER_SIZE

                    fileStream.Close();
                }

                using (var fs = new FileStream(persistenceFilePath, FileMode.Truncate)) { } // wipe the disk file


                // sort by DateTime.UtcNow, so the oldest cache entry is at the beginning, and GO
                this.stringArrayCache = stringArrayCache.OrderBy(row => row[0]); // row[0] is the datetime.now.ticks when it was added

                // rewrite the sorted array to file, so it matches the cache
                using (FileStream fileStream = new FileStream(persistenceFilePath, FileMode.Create))
                {
                    IFormatter bf = new BinaryFormatter();
                    bf.Serialize(fileStream, this.stringArrayCache);
                    fileStream.Close();
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
                Console.WriteLine("WARNING: Persistence file is empty or unexpected format  -- " + e.Message);
            }
            

            Console.WriteLine("INFO: In-Memory Cache Initialized with " + ringBufferSize + " records");

        }

        public void addMessage(string topic, string message, DateTime created)
        {

            if (indexToWrite > ringBufferSize - 1) // if we've reached the end of the ringbuffer / cache
                indexToWrite = 0;

            this.stringArrayCache[indexToWrite, 0] = created.Ticks.ToString(); // gives me a sortable  column
            this.stringArrayCache[indexToWrite, 1] = topic;
            this.stringArrayCache[indexToWrite, 2] = message;

            indexToWrite++; // set pointer to next index in the cache for next message write (seems ringbuffery to me)

            Console.WriteLine("INFO: topic: \"" + topic + "\", with message: \"" + message + "\" has been added to cache at \"" + created.Ticks.ToString() + "\"");

            //write to persisted cache disk file
            serializedFilePersist();
            Console.WriteLine("INFO: Cache persisted to disk file");

            //write to DB
            sqliteDatabasePersist();
            Console.WriteLine("INFO: Message added to sqlite database");

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

            // would be nice: use ORM wrapper like Dapper for query sanitization + better readability

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
        
    }


    //
    // below is just a class i found for sorting multidimensional arrays by column, which i need for when i restore
    //    the persisted disk cache back into memory
    // 

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


}
