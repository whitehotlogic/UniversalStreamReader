using Microsoft.VisualStudio.TestTools.UnitTesting;
using UniversalStreamReader;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UniversalStreamReader.Tests
{
    [TestClass()]
    public class SQLitePersistTests
    {

        const string dbpath = @"c:\temp\tempdb.sqlite";

        [TestMethod()]
        public void SQLitePersistTest()
        {
            IPersist sqlp = new SQLitePersist("");

            if (sqlp == null)
                Assert.Fail();
        }

        [TestMethod()]
        public void PersistTest()
        {
            
            IPersist sqlp = new SQLitePersist(dbpath);
            sqlp.Persist("topic","message","0");

            if (!File.Exists(dbpath))
                Assert.Fail();
        }

        [TestCleanup()]
        public void Teardown()
        {
            if (File.Exists(dbpath))
                File.Delete(dbpath);
        }
    }
}