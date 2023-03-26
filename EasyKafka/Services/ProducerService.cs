using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EasyKafka.Services;

public class ProducerService<T>
{
    internal readonly ProducerConfig _config;
    internal readonly ISchemaRegistryClient _schemaRegistryClient;
    internal readonly IProducer<string, T> _producer;
    internal readonly ILogger<ProducerService<T>> _logger;
    internal readonly string _topic;
    internal readonly string _producerName;

    public ProducerService(IConfiguration configuration, ILogger<ProducerService<T>> logger, string producerName)
    {
        _logger = logger;
        _producerName = producerName;
        _topic = configuration[$"Kafka:Producer:{producerName}:Topic"] ?? throw new ArgumentNullException($"Kafka:Producer:{producerName}:Topic");
        _logger.LogInformation("Creating kafka producer {producerName} for {topic}", _producerName, _topic);

        var kafkaConfigSection = configuration.GetSection($"Kafka:Producer:{producerName}");
        var kafkaBootstrapServers = kafkaConfigSection["BootstrapServers"] ?? throw new ArgumentNullException($"Kafka:Producer:{producerName}:BootstrapServers");
        var kafkaSchemaRegistryUrl = kafkaConfigSection["SchemaRegistryUrl"] ?? throw new ArgumentNullException($"Kafka:Producer:{producerName}:SchemaRegistryUrl");

        _config = new ProducerConfig
        {
            BootstrapServers = kafkaBootstrapServers,
            ClientId = producerName
        };

        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = kafkaSchemaRegistryUrl
        };

        _schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
        _producer = new ProducerBuilder<string, T>(_config)
            .SetValueSerializer(new AvroSerializer<T>(_schemaRegistryClient).AsSyncOverAsync())
            .Build();
    }

    public async Task ProduceAsync(string key, T value)
    {
        _logger.LogDebug("{producerName }: Producing message to {topic} with key {key}", _producerName, _topic,
            key ?? "null");
        await _producer.ProduceAsync(_topic, new Message<string, T> { Key = key, Value = value });
    }
}