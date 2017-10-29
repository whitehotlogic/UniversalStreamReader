using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Text;
using System.Data.SQLite;
using System.Threading;

namespace Integration.Tests
{
    [TestClass]
    public class IntegrationTest
    {

        [TestMethod]
        public void ConfirmMessagePathEndToEnd()
        {

            // run producer executable
            // subscribe to topic
            // produce message

            using (var sw = new StringWriter())
            {
                
                using (var sr = new StringReader("testMessageKey testMessageValue"))
                {
                    Console.SetOut(sw);
                    Console.SetIn(sr);

                    // Act
                    KafkaProducer.Program.Main(new string[0]);

                    // Assert
                    var result = sw.ToString();
                    if (!result.Contains("SUCCESS"))
                        Assert.Fail();
                }
             
            }

            // run consumer executable
            // subscribe to topic
            // if message not received in UI, assert.fail
            
            using (var sw = new StringWriter())
            {

                Console.SetOut(sw);

                // invoke 
                UniversalStreamReader.Program.Main(new string[1] { "100" }); // number of pollRepeat

                // check for test data in output
                var result = sw.ToString();
                if (!result.Contains("testMessageKey") ||
                    !result.Contains("testMessageValue"))
                    Assert.Fail();
            }
           

        }
    }
}
