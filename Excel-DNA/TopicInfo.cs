namespace Excel_DNA
{
    public class TopicInfo
    {
        public string TopicName { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }

        public TopicInfo(string topicName, int partition, long offset)
        {
            this.TopicName = topicName;
            this.Partition = partition;
            this.Offset = offset;
        }
    }
}