using System;
using System.IO;


namespace UniversalStreamReader
{
    public class FilePersist : IPersist
    {
        string persistenceFilePath;

        public FilePersist(string persistenceFilePath)
        {
            this.persistenceFilePath = persistenceFilePath;
        }

        public void Persist(string topic, string messageKey, string messageValue, long created)
        {
            try // check and see if cache persistence file exists, and if not, create it
            {
                if (!File.Exists(persistenceFilePath))
                    File.Create(persistenceFilePath).Dispose(); // create the cache persist file on disk
            }
            catch (IOException e)
            {
                Console.WriteLine("ERROR: Could not create cache persistence file -- " + e.Message);
                throw new Exception("Unable to persist message to file");
            }

            using (System.IO.StreamWriter persistFile = new System.IO.StreamWriter(persistenceFilePath, true))
            {
                persistFile.WriteLine($"{topic},{messageKey},{messageValue},{created}\n");
            }

            Console.WriteLine("INFO: Message persisted to file: " + persistenceFilePath);
        }
    }
}
