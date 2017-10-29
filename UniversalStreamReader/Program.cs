using System;
using System.Collections.Generic;

namespace UniversalStreamReader
{
    public class Program
    {

        const string DATABASE_FILE = @"c:\tempDev\DBPersist.sqlite";
        const string PERSISTENCE_FILE = @"c:\tempDev\FilePersist.csv";
        const string STREAM_SERVERS = "192.168.1.118:9092";
        const int RINGBUFFER_SIZE = 5;
         // for unit + integration tests

        public static void Main(string[] args)
        {

            // decide number of times to poll with consumer
            int pollRepeat = (args.Length > 0) ? Convert.ToInt32(args[0]) : -1; // -1 is infinite poll

            List<string> topicsToConsume = new List<string>();
            topicsToConsume.Add("maintopic"); // only subscribing to one topic for PoC

            IPersist[] ip = new IPersist[] { // all available types of persistence (easy to add more)
                new FilePersist(PERSISTENCE_FILE),
                new SQLitePersist(DATABASE_FILE)
            };
            ICache imc = new InMemoryCache(RINGBUFFER_SIZE); // create in-memory cache
            IStreamConsumer sck = new StreamConsumer_Kafka(imc, ip); // create new StreamConsumer with ringbuffer 
                                                                     //  and PoC persistence types
            
            sck.Run_Poll(STREAM_SERVERS, topicsToConsume, pollRepeat); //start main loop to consume kafka cluster

        }
    }
   
}
