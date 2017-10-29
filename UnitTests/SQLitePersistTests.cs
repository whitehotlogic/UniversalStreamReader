using Microsoft.VisualStudio.TestTools.UnitTesting;
using UniversalStreamReader;
using System.IO;
using System;

namespace UniversalStreamReader.Tests
{
    [TestClass()]
    public class SQLitePersistTests
    {

        const string dbpath = @"c:\tempDev\testdb.sqlite";

        [TestMethod()] // test constructor
        public void SQLitePersistTest()
        {
            IPersist sqlp = new SQLitePersist("");

            if (sqlp == null)
                Assert.Fail();
        }

        [TestMethod()] // test primary functionality
        public void PersistTest()
        {   
            IPersist sqlp = new SQLitePersist(dbpath);
            sqlp.Persist("topic","messageKey", "messageValue",0);
            sqlp = null;

            if (!File.Exists(dbpath))
                Assert.Fail();
        }

        [TestCleanup()]
        public void Teardown()
        {
            GC.Collect(); // garbage collect
            GC.WaitForPendingFinalizers(); // wait for database connection to be nulled

            if (File.Exists(dbpath))
                File.Delete(dbpath); // remove test database
        }
    }
}