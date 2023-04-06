using System.Collections.Generic;
using System.Data.SqlClient;
using ExcelDna.Integration;
using ExcelDna.Integration.Rtd;

namespace Excel_DNA
{
    public class KafkaRTD : ExcelRtdServer
    {
        private Dictionary<string, string> _topics = new Dictionary<string, string>();

        public KafkaRTD()
        {
            _topics.Add("k1", "test-topic1");
            _topics.Add("k2", "test-topic2");
        }

        public  object ConnectData(TopicInfo topic, IList<string> topicInfoList, ref bool newValues)
        {
            if (_topics.ContainsKey(topic.TopicName))
            {
                return GetLatestValue(topic.TopicName);
            }
            else
            {
                return ExcelError.ExcelErrorValue;
            }
        }


        private object GetLatestValue(string key)
        {
            var connectionString = "Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=MyDatabase;Integrated Security=True;";

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand("SELECT COUNT(*) FROM MyTable", connection))
                {
                    int count = (int)command.ExecuteScalar();

                    if (count != 0)
                    {
                        return count;
                    }
                    else
                    {
                        return ExcelError.ExcelErrorValue;
                    }
                }

              
            }
        }
    }

}
