using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Threading.Tasks;
using ExcelDna.Integration;
using KafkaNET;
using KafkaNET.Model;
using KafkaNET.Protocol;
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

        public override object ConnectData(TopicInfo topicInfo, ref bool newValues)
        {
            if (_topics.ContainsKey(topicInfo.TopicId))
            {
                return GetLatestValue(topicInfo.TopicId);
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

                var query = $"SELECT TOP 1 value FROM MyTable WHERE key = '{key}' ORDER BY time DESC";
                var value = connection.ExecuteScalar<double?>(query);

                if (value != null)
                {
                    return value.Value;
                }
                else
                {
                    return ExcelError.ExcelErrorValue;
                }
            }
        }
    }

}
