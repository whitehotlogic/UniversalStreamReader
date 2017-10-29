using System;
using System.Data.SQLite;
using System.IO;

namespace UniversalStreamReader
{
    public class SQLitePersist : IPersist
    {

        string dbFilePath;

        public SQLitePersist(string dbFilePath)
        {
            this.dbFilePath = dbFilePath;
        }

        public void Persist(string topic, string messageKey, string messageValue, long created)
        {
            // REFACTOR TODO: split db conn, table creates, inserts into different methods
            //     -- can be better refactored as a repository

            // connect to db and create necessary objects
            SQLiteConnection dbConnection = new SQLiteConnection("Data Source=" + dbFilePath + ";Version=3;");
            dbConnection.Open();
            string tsql = null;
            SQLiteCommand dbCommand = null;

            try // create message table if it doesn't already exist
            {
                tsql = "create table if not exists KafkaMessages (" +
                    "idMessage INTEGER PRIMARY KEY ASC, " +
                    "topic VARCHAR not null," +
                    "messageKey VARCHAR, " + // kafka message key can be null
                    "messageValue VARCHAR, " + // kafka message value can be null
                    "created INTEGER not null UNIQUE )";
                dbCommand = new SQLiteCommand(tsql, dbConnection);
                dbCommand.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                Console.WriteLine("ERROR: Problem creating table \"KafkaMessages\" -- " + e.Message);
            }


            try
            {
                // insert message in the Messages table if the row doesn't exist already
                tsql = "insert or IGNORE into KafkaMessages (topic,messageKey,messageValue,created) " +
                    $"values ('{topic}','{messageKey}','{messageValue}',{created})";
                dbCommand = new SQLiteCommand(tsql, dbConnection);
                dbCommand.ExecuteNonQuery();
            }
            catch (SQLiteException e)
            {
                Console.WriteLine("ERROR: Problem inserting message -- " + e.Message);
            }

            dbConnection.Close();
            Console.WriteLine("INFO: Message persisted to SQLite database: " + dbFilePath);

        }

    }
}
