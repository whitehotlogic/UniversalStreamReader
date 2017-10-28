using System.Collections.Generic;

namespace UniversalStreamReader
{
    public interface IStreamConsumer
    {
        void Run_Poll(string server, List<string> topicList);
    }
}
