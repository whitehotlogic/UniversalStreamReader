using Microsoft.VisualStudio.TestTools.UnitTesting;


namespace UniversalStreamReader.Tests
{
    [TestClass()]
    public class InMemoryCacheTests
    {
        [TestMethod()] // test constructor
        public void InMemoryCacheTest()
        {
            ICache imc = new InMemoryCache(5);
            if (imc == null)
            {
                Assert.Fail();
            }
        }

        [TestMethod()] // test primary functionality
        public void addMessageTest()
        {
            // TODO
            Assert.Fail();
        }
    }
}