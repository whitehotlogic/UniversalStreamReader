using Microsoft.VisualStudio.TestTools.UnitTesting;


namespace UniversalStreamReader.Tests
{
    [TestClass()]
    public class InMemoryCacheTests
    {
        [TestMethod()]
        public void InMemoryCacheTest()
        {
            ICache imc = new InMemoryCache(5);
            if (imc == null)
            {
                Assert.Fail();
            }
        }

        [TestMethod()]
        public void addMessageTest()
        {
            Assert.Fail();
        }
    }
}