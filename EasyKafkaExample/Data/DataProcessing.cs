using Confluent.Kafka;

namespace EasyKafkaExample.Data;

public class ProcessData<T>
{
    public static void OnMessageReceived(Message<string, T> message)
    {
        // Console.WriteLine write the message key and value
        Console.WriteLine("Message received: {0} {1}", message.Key, message.Value);
    }
}