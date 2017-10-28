using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Data.SQLite;
using Confluent.Kafka.Serialization;
using Confluent.Kafka;


namespace UniversalStreamReader
{
    class Program
    {
        public const string DATABASE_FILE = @"c:\tempDev\DBPersist.sqlite";
        public const string STREAM_SERVERS = "192.168.1.118:9092";

        public const string PERSISTENCE_FILE = @"c:\tempDev\FilePersist.csv";
        public const int RINGBUFFER_SIZE = 5;

        static void Main(string[] args)
        {

            List<string> topicsToConsume = new List<string>();
            topicsToConsume.Add("testaroo");

            IPersist[] ip = new IPersist[] {
                new FilePersist(PERSISTENCE_FILE),
                new SQLitePersist(DATABASE_FILE)
            };
            InMemoryCache imc = new InMemoryCache(RINGBUFFER_SIZE);
            IStreamConsumer sck = new StreamConsumer_Kafka(imc, ip);

            //main loop to consume kafka cluster
            sck.Run_Poll(STREAM_SERVERS, topicsToConsume);
           
        }
    }

    /**
     * 
     * 
     * 
     * 
     *   Stream Reader Interface and Implementations
     *   
     *   
     *   
     *   
     */

    interface IStreamConsumer
    {
        void Run_Poll(string server, List<string> topicList);
    }

    class StreamConsumer_Kafka : IStreamConsumer {

        private InMemoryCache imc;
        private IPersist[] ip;


        public StreamConsumer_Kafka(InMemoryCache imc, IPersist[] ip)
        {
            this.imc = imc;
            this.ip = ip;
        }


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


        public void Run_Poll(string brokerList, List<string> topics)
        {

            using (var consumer = new Consumer<string, string>(constructConfig(brokerList, true), new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                
                consumer.OnMessage += (_, msg) => // NEW MESSAGE FOUND FOR TOPIC!
                {

                    Console.WriteLine($"NEW_MESSAGE: Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");

                    string created = DateTime.Now.Ticks.ToString(); // just some numeric value based on time, jic i need to sort at any point

                    imc.addMessage(msg.Topic, msg.Value, created); //write to in-memory cache

                    foreach (IPersist persistentStorageImplementation in ip) //perform each storage implementation for this message
                    {
                        persistentStorageImplementation.Persist(msg.Topic, msg.Value, created);
                    }
                    
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
     *   Persistence Interface and Implementations
     *   
     *   
     *   
     *   
     */


    interface IPersist
    {
        void Persist(string topic, string message, string created);

    }

    class FilePersist : IPersist
    {
        string persistenceFilePath;

        public FilePersist(string persistenceFilePath)
        {
            this.persistenceFilePath = persistenceFilePath;

            try // check and see if cache persistence file exists, and if not, create it
            {
                if (!File.Exists(persistenceFilePath))
                {
                    File.Create(persistenceFilePath).Dispose(); // create the cache persist file on disk
                }
            }
            catch (IOException e)
            {
                Console.WriteLine("ERROR: Could not create cache persistence file -- " + e.Message);
            }
        }

        public void Persist(string topic, string message, string created)
        {
            /*
            using (FileStream fileStream = new FileStream(persistenceFilePath, FileMode.Create))
            {
                IFormatter bf = new BinaryFormatter();
                bf.Serialize(fileStream, );
                fileStream.Close();
            }
            */

            using (System.IO.StreamWriter persistFile = new System.IO.StreamWriter(persistenceFilePath, true))
            {
                persistFile.WriteLine(topic + "," + message + "," + created + "\n");
            }

            Console.WriteLine("INFO: Message persisted to file: " + persistenceFilePath);
        }
    }

    class SQLitePersist : IPersist
    {
        string dbFilePath;

        public SQLitePersist(string dbFilePath)
        {
            this.dbFilePath = dbFilePath;
        }

        public void Persist(string topic, string message, string created)
        {

            // create db if it doesn't already exists
            if (!File.Exists(dbFilePath))
                SQLiteConnection.CreateFile(dbFilePath);
            // IS THE ABOVE EVEN NECESSARY? DOESN'T SQLITCONNECTION DO THIS AUTOMAGICALLY?

            // connect to db and create necessary objects
            SQLiteConnection dbConnection = new SQLiteConnection("Data Source=" + dbFilePath + ";Version=3;");
            dbConnection.Open();
            string tsql = null;
            SQLiteCommand dbCommand = null;

            try  // create topic table if it doesn't already exist
            {
                tsql = "create table if not exists Topics (" +
                    "idTopic INTEGER PRIMARY KEY ASC, " +
                    "topic VARCHAR not null UNIQUE)";
                dbCommand = new SQLiteCommand(tsql, dbConnection);
                dbCommand.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                Console.WriteLine("ERROR: Problem creating table \"Topics\" -- " + e.Message);
            }

            try // create message table if it doesn't already exist
            {
                tsql = "create table if not exists Messages (" +
                    "idMessage INTEGER PRIMARY KEY ASC, " +
                    "message VARCHAR not null, " +
                    "created INTEGER not null UNIQUE, " +
                    "idTopic INTEGER not null," +
                    "FOREIGN KEY(idTopic) REFERENCES Topics(idTopic) )";
                dbCommand = new SQLiteCommand(tsql, dbConnection);
                dbCommand.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                Console.WriteLine("ERROR: Problem creating table \"Messages\" -- " + e.Message);
            }

            try // insert topic in Topics table if the row doesn't exist already
            {
                tsql = "insert or IGNORE into Topics (topic) values ('" + topic + "') ";
                dbCommand = new SQLiteCommand(tsql, dbConnection);
                dbCommand.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                Console.WriteLine("ERROR: Problem creating new topic -- " + e.Message);
            }

            try 
            {
                // get idTopic from topic table, to be used as foreign key for message insert
                tsql = "select idTopic from Topics where " +
                    "topic = '" + topic + "'";
                dbCommand = new SQLiteCommand(tsql, dbConnection);
                SQLiteDataReader dbReader = dbCommand.ExecuteReader();
                dbReader.Read();
                string idTopic = dbReader.GetInt32(0).ToString();

                // insert message in the Messages table if the row doesn't exist already
                tsql = "insert or IGNORE into Messages (message,created,idTopic) " +
                    "values ('" + message + "'," + created + "," + idTopic + ")";
                dbCommand = new SQLiteCommand(tsql, dbConnection);
                dbCommand.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                Console.WriteLine("ERROR: Problem inserting message -- " + e.Message);
            }

            Console.WriteLine("INFO: Message persisted to SQLite database: " + dbFilePath);

        }

    }

    /**
     * 
     * 
     * 
     * 
     *   In-Memory Cache Implementation
     *   
     *   
     *   
     *   
     */

    class InMemoryCache
    {

        private string[,] stringArrayCache = null;
        private int indexToWrite = 0;
        private int ringBufferSize;

        public InMemoryCache(int ringBufferSize)
        {

            this.ringBufferSize = ringBufferSize;
            stringArrayCache = new string[ringBufferSize, 3]; // number of rows in array predefined by ringBufferSize
                                                              // three strings are created, topic, message
        }

        public void addMessage(string topic, string message, string created)
        {

            if (indexToWrite > ringBufferSize - 1) // if we've reached the end of the ringbuffer / cache
                indexToWrite = 0;

            this.stringArrayCache[indexToWrite, 0] = created; // gives me a sortable  column
            this.stringArrayCache[indexToWrite, 1] = topic;
            this.stringArrayCache[indexToWrite, 2] = message;

            indexToWrite++; // set pointer to next index in the cache for next message write (seems ringbuffery to me)

            Console.WriteLine("INFO: topic: \"" + topic + "\", with message: \"" + message + "\" has been added to cache at \"" + created + "\"");

        }

    }


}
