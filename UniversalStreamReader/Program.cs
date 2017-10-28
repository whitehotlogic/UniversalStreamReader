using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Data.SQLite;
using Confluent.Kafka.Serialization;
using Confluent.Kafka;


namespace UniversalStreamReader
{
    class Program
    {

        const string DATABASE_FILE = @"c:\tempDev\DBPersist.sqlite";
        const string PERSISTENCE_FILE = @"c:\tempDev\FilePersist.csv";
        const string STREAM_SERVERS = "192.168.1.118:9092";
        const int RINGBUFFER_SIZE = 5;

        static void Main(string[] args)
        {

            List<string> topicsToConsume = new List<string>();
            topicsToConsume.Add("maintopic"); // only subscribing to one topic for PoC

            IPersist[] ip = new IPersist[] {
                new FilePersist(PERSISTENCE_FILE),
                new SQLitePersist(DATABASE_FILE)
            };
            ICache imc = new InMemoryCache(RINGBUFFER_SIZE);
            IStreamConsumer sck = new StreamConsumer_Kafka(imc, ip);

            //main loop to consume kafka cluster
            sck.Run_Poll(STREAM_SERVERS, topicsToConsume);
           
        }
    }
   
}
