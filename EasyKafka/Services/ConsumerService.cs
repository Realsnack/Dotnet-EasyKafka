using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EasyKafka.Services;

public class ConsumerService<T> : IHostedService
{
    /// <summary>
    /// Kafka Consumer configuration
    /// </summary>
    internal ConsumerConfig? Config;

    /// <summary>
    /// Instance of the Schema Registry Client
    /// </summary>
    internal ISchemaRegistryClient SchemaRegistryClient;

    /// <summary>
    /// Kafka Consumer
    /// </summary>
    private readonly IConsumer<string, T> _consumer;

    private readonly ILogger<ConsumerService<T>> _logger;

    /// <summary>
    /// Consumed topic
    /// </summary>
    internal string? Topic;

    /// <summary>
    /// Consumer name used for logging and configuration
    /// </summary>
    internal readonly string ConsumerName;

    /// <summary>
    /// Method to be called when a message is received
    /// </summary>
    private readonly Action<Message<string, T>> _onMessageReceived;

    /// <summary>
    /// Consumer task
    /// </summary>
    private Task _consumerTask;

    /// <summary>
    /// Used to cancel the consumer task
    /// </summary>
    private CancellationTokenSource _cancellationTokenSource;

    /// <summary>
    /// Consumer service constructor
    /// </summary>
    /// <param name="configuration">IConfiguration implementation that provides configuration for Kafka and SchemaRegistry</param>
    /// <param name="logger">Implementation of ILogger</param>
    /// <param name="consumerName">Name of created consumer - will be used in configuration</param>
    /// <param name="onMessageReceived">Method to be called when a message is received</param>
    /// <param name="config">Supply own configuration if the default kafka configuration isn't </param>
    /// <param name="schemaRegistryConfig">Supply own configuration if the default schemaRegistry config doesn't provide enough configuration</param>
    public ConsumerService(IConfiguration configuration, ILogger<ConsumerService<T>> logger,
        string consumerName,
        Action<Message<string, T>> onMessageReceived,
        ConsumerConfig? config = null, SchemaRegistryConfig? schemaRegistryConfig = null)
    {
        _logger = logger;
        ConsumerName = consumerName;
        _onMessageReceived = onMessageReceived;

        LoadConfiguration(configuration, consumerName, config, schemaRegistryConfig);
        _consumer = CreateConsumer();
    }

    /// <summary>
    /// Method called when the service is started to create instance of IConsumer
    /// </summary>
    /// <returns>IConsumer instance</returns>
    private IConsumer<string, T> CreateConsumer()
    {
        if (typeof(T) == typeof(string))
        {
            return new ConsumerBuilder<string, T>(Config)
                .Build();
        }

        return new ConsumerBuilder<string, T>(Config)
            .SetValueDeserializer(new AvroDeserializer<T>(SchemaRegistryClient).AsSyncOverAsync())
            .Build();
    }

    /// <summary>
    /// Method to load configuration for the service
    /// </summary>
    /// <param name="configuration"></param>
    /// <param name="producerName"></param>
    /// <param name="config"></param>
    /// <param name="schemaRegistryConfig"></param>
    /// <exception cref="ArgumentNullException">Exception thrown when configuration property is missing in default configuration implementation</exception>
    internal void LoadConfiguration(IConfiguration configuration, string producerName, ConsumerConfig? config = null,
        SchemaRegistryConfig? schemaRegistryConfig = null)
    {
        Topic = configuration[$"Kafka:Consumer:{producerName}:Topic"] ??
                throw new ArgumentNullException($"Kafka:Consumer:{producerName}:Topic");
        _logger.LogInformation("Creating kafka producer {producerName} for {topic}", ConsumerName, Topic);

        var kafkaConfigSection = configuration.GetSection($"Kafka:Consumer:{producerName}");
        var kafkaBootstrapServers = kafkaConfigSection["BootstrapServers"] ??
                                    throw new ArgumentNullException($"Kafka:Consumer:{producerName}:BootstrapServers");
        var kafkaSchemaRegistryUrl = kafkaConfigSection["SchemaRegistryUrl"] ??
                                     throw new ArgumentNullException(
                                         $"Kafka:Producer:{producerName}:SchemaRegistryUrl");

        Config = config ?? new ConsumerConfig
        {
            BootstrapServers = kafkaBootstrapServers,
            GroupId = producerName,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        schemaRegistryConfig ??= new SchemaRegistryConfig
        {
            Url = kafkaSchemaRegistryUrl
        };
        SchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
    }

    /// <summary>
    /// Starts the consumer
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _consumer.Subscribe(Topic);
        _logger.LogInformation("{consumerName}: Subscribed to {topic}", ConsumerName, Topic);

        // Offload the consumer processing to a separate thread
        _consumerTask = Task.Run(() => RunWorkerAsync(_cancellationTokenSource.Token), cancellationToken);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Runs the consumer loop
    /// </summary>
    /// <param name="cancellationToken"></param>
    private async Task RunWorkerAsync(CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("{ConsumerName}: Starting consumer loop", ConsumerName);
            while (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogDebug("Waiting for messages...");
                var consumeResult = _consumer.Consume(cancellationToken);
                if (consumeResult.IsPartitionEOF)
                {
                    _logger.LogInformation(
                        "{consumerName}: Reached end of topic {topic}, partition {partition}, offset {offset}.",
                        ConsumerName, consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);
                    continue;
                }

                _logger.LogInformation(
                    "{consumerName}: Consumed message from {topic}, partition {partition}, offset {offset}.",
                    ConsumerName, consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);

                _onMessageReceived(consumeResult.Message);

                // Add a small delay to prevent high CPU usage while waiting for new messages
                await Task.Delay(100, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Do not log the error if the operation was canceled
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Exception occurred while consuming messages");
            throw;
        }
    }

    /// <summary>
    /// Stops the consumer
    /// </summary>
    /// <param name="cancellationToken"></param>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_consumerTask != null)
        {
            // Signal the consumer task to stop
            _cancellationTokenSource.Cancel();

            // Wait for the consumer task to complete or the cancellation token to be triggered
            await Task.WhenAny(_consumerTask, Task.Delay(Timeout.Infinite, cancellationToken));

            // Dispose the task only if it has completed
            if (_consumerTask.Status == TaskStatus.RanToCompletion ||
                _consumerTask.Status == TaskStatus.Faulted ||
                _consumerTask.Status == TaskStatus.Canceled)
            {
                _consumerTask.Dispose();
            }

            _logger.LogInformation("Consumer {consumerName} stopped", ConsumerName);
        }
    }
}
