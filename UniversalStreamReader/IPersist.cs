namespace UniversalStreamReader
{
    public interface IPersist
    {
        void Persist(string topic, string messageKey, string messageValue, long created);

    }
}
