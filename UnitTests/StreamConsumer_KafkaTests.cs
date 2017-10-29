using Microsoft.VisualStudio.TestTools.UnitTesting;
using UniversalStreamReader;
using System;
using System.Collections.Generic;

namespace UniversalStreamReader.Tests
{

    [TestClass()]
    public class StreamConsumer_KafkaTests
    {
        private class MockMemoryCache : ICache
        {
            public void addMessage(string topic, string messageKey, string messageValue, long created)
            {
                throw new NotImplementedException();
            }
        }

        [TestMethod()]
        public void StreamConsumer_KafkaTest() // test constructor
        {
            ICache ic = new MockMemoryCache();
            IStreamConsumer sck = new StreamConsumer_Kafka(ic, new IPersist[0]);
            if (sck == null)
            {
                Assert.Fail();
            }
        }

        [TestMethod()] // test primary functionality
        public void Run_PollTest()
        {

            ICache ic = new MockMemoryCache();
            IStreamConsumer sck = new StreamConsumer_Kafka(ic, new IPersist[0]);

            int result = sck.Run_Poll("0.0.0.0", new List<string> { "test" }, 1);

            if (result!=0) // something bad happened and Run_Poll didn't reach end of code block
                Assert.Fail();

        }




    }
}