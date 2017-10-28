using System;


namespace UniversalStreamReader
{
    public class InMemoryCache : ICache
    {

        private string[,] stringArrayCache = null;
        private int indexToWrite = 0;
        private int ringBufferSize;

        public InMemoryCache(int ringBufferSize)
        {

            this.ringBufferSize = ringBufferSize;
            stringArrayCache = new string[ringBufferSize, 4]; // number of rows in array predefined by ringBufferSize
                                                              // three strings are: topic, key, message, created
        }

        public void addMessage(string topic, string messageKey, string messageValue, long created)
        {

            if (indexToWrite > ringBufferSize - 1) // if we've reached the end of the ringbuffer / cache
                indexToWrite = 0;

            this.stringArrayCache[indexToWrite, 0] = created.ToString(); // sortable age column, if needed
            this.stringArrayCache[indexToWrite, 1] = topic;
            this.stringArrayCache[indexToWrite, 2] = messageKey;
            this.stringArrayCache[indexToWrite, 2] = messageValue;

            indexToWrite++; // set pointer to next index in the circular cache for next message write

            Console.WriteLine("INFO: topic: \"" + topic + "\", with message: \"" + messageKey + "\", \"" +
                messageValue + "\" has been added to cache at \"" + created + "\"");

        }

    }
}
