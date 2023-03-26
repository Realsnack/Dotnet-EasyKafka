using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EasyKafka.Services;

public class ProducerService<T>
{
	private readonly ProducerConfig _config;
	private readonly ISchemaRegistryClient _schemaRegistryClient;
	private readonly IProducer<string, T> _producer;
	private readonly ILogger<ProducerService<T>> _logger;
	private readonly string _topic;
	private readonly string _producerName;

	public ProducerService(IConfiguration configuration, ILogger<ProducerService<T>> logger, string producerName)
	{
		_logger = logger;
		_producerName = producerName;

		_topic = $"Kafka:Producer:{producerName}:Topic";
		_logger.LogInformation("Creating kafka producer {producerName} for {topic}", _producerName, _topic);
		
		_config = new ProducerConfig();
		var kafkaConfigName = $"Kafka:Producer:{producerName}";
		configuration.GetSection(kafkaConfigName).Bind(_config);

		var schemaRegistryConfig = new SchemaRegistryConfig();
		const string schemaRegistryConfigName = $"Kafka:SchemaRegistry";
		configuration.GetSection(schemaRegistryConfigName).Bind(schemaRegistryConfig);
		_schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
		
		_producer = new ProducerBuilder<string, T>(_config)
			.SetValueSerializer(new AvroSerializer<T>(_schemaRegistryClient).AsSyncOverAsync())
			.Build();
	}

	public async Task ProduceAsync(string topic, string key, T value)
	{
		_logger.LogDebug("{producerName }: Producing message to {topic} with key {key}", _producerName, topic, key ?? "null");
		await _producer.ProduceAsync(topic, new Message<string, T> { Key = key, Value = value });
	}
}