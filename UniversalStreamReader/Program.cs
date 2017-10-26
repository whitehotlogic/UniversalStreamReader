using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

using System.Threading.Tasks; // will be used soon

namespace UniversalStreamReader
{
    class Program
    {
        static void Main(string[] args)
        {

            //create background thread to manage cache policy

            InMemoryCacheWithPersist imcwp = new InMemoryCacheWithPersist("c:\\tempDev\\InMemoryCacheWithPersist.txt");

            imcwp.add("topic2", "message2");

            Console.Out.WriteLine(imcwp.get("topic2"));

            //on close, persist to disk
            imcwp.persist();

            Task.Delay(TimeSpan.FromSeconds(5)).Wait(); // so i can see the output for now
        }
    }


    /**
     * 
     * 
     * 
     *    Message Object
     * 
     * 
     * 
     * 
     */

    class KafkaMessage
    {
        public string Value;
        public string Created;
    }

    /**
     * 
     * 
     * 
     * 
     *   Trivial In-Memory Cache Implementation
     *   
     *   -- needs better locking?
     *   
     *   
     */


    class InMemoryCacheWithPersist
    {
        //ConcurrentDictionary<string,List<KafkaMessage>> cache = null;
        ConcurrentDictionary<string,string> cache = null; // use ConcurrentDictionary because it's thread-safe with atomic ops, and O(1)
        private String persistenceFilePath = null;

        public InMemoryCacheWithPersist(string persistenceFilePath)
        {

            this.cache = new ConcurrentDictionary<string,string>(); //for initial testing, use strings for key/value
            //this.cache = new ConcurrentDictionary<string,List<KafkaMessage>>();
            this.persistenceFilePath = persistenceFilePath;

            try // will get FileNotFoundException if no cache available on disk
            {
                using (FileStream fileStream = new FileStream(persistenceFilePath, FileMode.Open))
                {
                    IFormatter bf = new BinaryFormatter();
                    //this.cache = (ConcurrentDictionary<string,List<KafkaMessage>>)bf.Deserialize(fileStream);
                    this.cache = (ConcurrentDictionary<string,string>)bf.Deserialize(fileStream);
                    fileStream.Close();
                }
            }
            catch (FileNotFoundException e)
            {
                Console.Out.WriteLine("Persistence File Not Found  -- " + e.Message);
            }
            catch (SerializationException e)
            {
                Console.Out.WriteLine("Persistence File is Empty  -- " + e.Message);
            }

        }


        public int size()
        {
            return this.cache.Count; // needed for ringbuffer byCount option? nope, this is wrong ---->
                                              // ----> need to ringbuffer by message count per topic, not topic (key) count 
        }


        public string get(string key)
        {
            if (this.cache.ContainsKey(key))
            {
                return cache[key] as string; // will be used by visualization
            } else
            {
                return ""; // returning an empty string for testing only
            }
            
        }


        public void add(string key, string value)
        {
            if (!this.cache.ContainsKey(key))
            {
                this.cache.TryAdd(key, value); //should be using AddOrUpdate() of course, this is just for initial testing
            }

            // replace above with better functionality:
            //this.cache.AddOrUpdate();
        }

        public void remove(string key, string value)
        {

        }


        public void persist()
        {
            using (FileStream fileStream = new FileStream(persistenceFilePath, FileMode.Create))
            {
                IFormatter bf = new BinaryFormatter();
                bf.Serialize(fileStream, this.cache);
                fileStream.Close();
            }
        }
    }

}
