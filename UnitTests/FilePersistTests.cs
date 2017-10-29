using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;

namespace UniversalStreamReader.Tests
{
    [TestClass()]
    public class FilePersistTests
    {
        string filepath = @"c:\tempDev\test.csv";

        [TestMethod()] // test constructor
        public void FilePersistTest()
        {
            IPersist fp = new FilePersist(filepath);
            if (fp == null)
                Assert.Fail();
        }

        [TestMethod()] // test primary functionality
        public void PersistTest()
        {
            IPersist fp = new FilePersist(filepath);
            fp.Persist("topic","messageKey","messageValue", 0);
            fp = null;

            if (!File.Exists(filepath))
                Assert.Fail();

        }

        [TestCleanup()]
        public void Teardown()
        {
            GC.Collect(); // garbage collect
            GC.WaitForPendingFinalizers(); // wait for database connection to be nulled

            if (File.Exists(filepath)) // remove test persistence file
                File.Delete(filepath);
        }
    }
}