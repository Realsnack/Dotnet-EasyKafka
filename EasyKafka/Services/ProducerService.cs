using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EasyKafka.Services;

public class ProducerService<T> : IDisposable
{
    internal ProducerConfig? Config;
    internal ISchemaRegistryClient? SchemaRegistryClient;
    private readonly IProducer<string?, T> _producer;
    private readonly ILogger<ProducerService<T>> _logger;
    internal string? Topic;
    internal readonly string ProducerName;

    public ProducerService(IConfiguration configuration, ILogger<ProducerService<T>> logger, string producerName, ProducerConfig? config = null, SchemaRegistryConfig? schemaRegistryConfig = null)
    {
        _logger = logger;
        ProducerName = producerName;
        LoadConfiguration(configuration, producerName, config, schemaRegistryConfig);

        _producer = new ProducerBuilder<string?, T>(Config)
            .SetValueSerializer(new AvroSerializer<T>(SchemaRegistryClient).AsSyncOverAsync())
            .Build();
    }

    internal void LoadConfiguration(IConfiguration configuration, string producerName, ProducerConfig? config = null, SchemaRegistryConfig? schemaRegistryConfig = null)
    {
        Topic = configuration[$"Kafka:Producer:{producerName}:Topic"] ?? throw new ArgumentNullException($"Kafka:Producer:{producerName}:Topic");
        _logger.LogInformation("Creating kafka producer {producerName} for {topic}", ProducerName, Topic);

        var kafkaConfigSection = configuration.GetSection($"Kafka:Producer:{producerName}");
        var kafkaBootstrapServers = kafkaConfigSection["BootstrapServers"] ?? throw new ArgumentNullException($"Kafka:Producer:{producerName}:BootstrapServers");
        var kafkaSchemaRegistryUrl = kafkaConfigSection["SchemaRegistryUrl"] ?? throw new ArgumentNullException($"Kafka:Producer:{producerName}:SchemaRegistryUrl");

        Config = config ?? new ProducerConfig
        {
            BootstrapServers = kafkaBootstrapServers,
            ClientId = producerName
        };

        schemaRegistryConfig ??= new SchemaRegistryConfig
        {
            Url = kafkaSchemaRegistryUrl
        };
        SchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
    }

    public async Task ProduceAsync(string? key, T value)
    {
        _logger.LogInformation("{producerName}: Producing message to {topic} with key {key}", ProducerName, Topic,
            key ?? "null");
        await _producer.ProduceAsync(Topic, new Message<string?, T> { Key = key, Value = value });
    }
    
    public void Dispose()
    {
        _producer.Dispose();
        SchemaRegistryClient?.Dispose();
        GC.SuppressFinalize(this);
    }
}