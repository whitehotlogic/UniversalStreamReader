using Microsoft.VisualStudio.TestTools.UnitTesting;
using UniversalStreamReader;
using System;

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
            // TODO
            Assert.Fail();

        }




    }
}