using Confluent.Kafka;

namespace ConsumerApp;

public class Program
{
    private static void Main()
    {
        var config = new Dictionary<string, string>
        {
            { "bootstrap.servers", "localhost:64294" },
            { "group.id", "kafka-dotnet-getting-started"},
            { "auto.offset.reset", "earliest" },
        };

        const string topic = "purchases";

        CancellationTokenSource cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, string>(config.AsEnumerable()).Build())
        {
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    var cr = consumer.Consume(cts.Token);
                    Console.WriteLine($"Consumed event from topic {topic}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
                }
            }
            catch (OperationCanceledException)
            {
                // Ctrl-C was pressed.
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
