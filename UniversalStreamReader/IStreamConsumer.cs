using System.Collections.Generic;

namespace UniversalStreamReader
{
    public interface IStreamConsumer
    {
        int Run_Poll(string server, List<string> topicList, int pollRepeat);
    }
}
