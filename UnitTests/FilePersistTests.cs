using Microsoft.VisualStudio.TestTools.UnitTesting;
using UniversalStreamReader;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace UniversalStreamReader.Tests
{
    [TestClass()]
    public class FilePersistTests
    {
        string filepath = @"c:\temp\temp.csv";

        [TestMethod()]
        public void FilePersistTest()
        {

            IPersist fp = new FilePersist(filepath);
            if (fp == null)
                Assert.Fail();
        }

        [TestMethod()]
        public void PersistTest()
        {
            try
            {
                IPersist fp = new FilePersist(filepath);
            }
            catch (Exception)
            {
                Assert.Fail();
            }
        }

        //teardown
        // remove file
        [TestCleanup()]
        public void Cleanup()
        {
            string FilePersistTestPath = filepath;
            if (File.Exists(FilePersistTestPath))
            {
                File.Delete(FilePersistTestPath);
            }
        }
    }
}