using System.ComponentModel;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EasyKafka.Services;

public class ConsumerService<T> : BackgroundWorker
{
    private readonly ConsumerConfig _config;
    private readonly ISchemaRegistryClient _schemaRegistryClient;
    private readonly IConsumer<string, T> _consumer;
    private readonly ILogger<ConsumerService<T>> _logger;
    private readonly string _topic;
    private readonly string _consumerName;
    private readonly Action<Message<string, T>> _onMessageReceived;

    public ConsumerService(IConfiguration configuration, ILogger<ConsumerService<T>> logger, string consumerName, Action<Message<string, T>> onMessageReceived)
    {
        _logger = logger;
        _consumerName = consumerName;
        _onMessageReceived = onMessageReceived;

        _topic = $"Kafka:Consumer:{consumerName}:Topic";
        _logger.LogInformation("Creating kafka consumer {consumerName} for {topic}", consumerName, _topic);

        _config = new ConsumerConfig();
        var kafkaConfigName = $"Kafka:Consumer:{consumerName}";
        configuration.GetSection(kafkaConfigName).Bind(_config);

        var schemaRegistryConfig = new SchemaRegistryConfig();
        const string schemaRegistryConfigName = $"Kafka:SchemaRegistry";
        configuration.GetSection(schemaRegistryConfigName).Bind(schemaRegistryConfig);
        _schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);

        _consumer = new ConsumerBuilder<string, T>(_config)
            .SetValueDeserializer(new AvroDeserializer<T>(_schemaRegistryClient).AsSyncOverAsync())
            .Build();
    }

    public void Start()
    {
        _consumer.Subscribe(_topic);
        _logger.LogInformation("{consumerName}: Subscribed to {topic}", _consumerName, _topic);
        RunWorkerAsync();
    }

    private async Task RunWorkerAsync()
    {
        while (!CancellationPending)
        {
            var consumeResult = _consumer.Consume();
            if (consumeResult.IsPartitionEOF)
            {
                _logger.LogInformation(
                    "{consumerName}: Reached end of topic {topic}, partition {partition}, offset {offset}.",
                    _consumerName, consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);
                continue;
            }

            _logger.LogInformation(
                "{consumerName}: Consumed message from {topic}, partition {partition}, offset {offset}.",
                _consumerName, consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);
            
            _onMessageReceived(consumeResult.Message);
        }
    }
}