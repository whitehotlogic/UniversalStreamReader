using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UniversalStreamReader
{
    public interface ICache
    {
        void addMessage(string topic, string messageKey, string messageValue, long created);
    }
}
