using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

using System.Threading.Tasks;
using System.Linq;

namespace UniversalStreamReader
{
    class Program
    {
        static void Main(string[] args)
        {

            const string PERSISTENCE_FILE = @"c:\tempDev\InMemoryCacheWithPersist.txt";

            //initialize cache
            InMemoryCacheWithPersist imcwp = new InMemoryCacheWithPersist(PERSISTENCE_FILE);

            //create background thread to manage the cache's ringbuffer policy
            //Parallel.Invoke(() => imcwp.ringBufferPolicer(1,3)); // Parallel.Invoke for best threading readability in this use case
            Task.Factory.StartNew(() => imcwp.ringBufferPolicer(1, 3));


            // add some dummy topics and messages for testing
            int i = 0;
            while (true)
            {

                string topic = "dummytopic" + i; // create hash for topic
                for (int j = 0; j < 20; j++) { // add 20 dummy messages to each topic
                    string message = "dummymessage" + j;
                    imcwp.add(topic, message, DateTime.Now);
                }
                
                Task.Delay(TimeSpan.FromSeconds(0.1)).Wait();

                //on close, persist to disk
                imcwp.persist();

                i++;
            }

            Task.Delay(TimeSpan.FromSeconds(10)).Wait(); // so i can see the output for now

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

                Console.Out.WriteLine("Loop of Background Thread");
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
