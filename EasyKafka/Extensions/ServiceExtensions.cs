using Confluent.Kafka;
using Confluent.SchemaRegistry;
using EasyKafka.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

namespace EasyKafka.Extensions;

public static class ServiceExtensions
{
    public static IServiceCollection AddEasyKafkaProducer<T>(
        this IServiceCollection services,
        string producerName,
        ProducerConfig? config = null,
        SchemaRegistryConfig? schemaRegistryConfig = null)
    {
        services.AddSingleton<ProducerService<T>>(serviceProvider =>
        {
            var configuration = serviceProvider.GetRequiredService<IConfiguration>();
            var logger = serviceProvider.GetRequiredService<ILogger<ProducerService<T>>>();
            var configInstance = config ?? new ProducerConfig();
            var schemaRegistryConfigInstance = schemaRegistryConfig ?? new SchemaRegistryConfig();

            return new ProducerService<T>(configuration, logger, producerName, configInstance,
                schemaRegistryConfigInstance);
        });

        return services;
    }

    public static IServiceCollection AddEasyKafkaProducer<T>(
        this IServiceCollection services,
        string producerName)
    {
        services.AddSingleton<ProducerService<T>>(serviceProvider =>
        {
            var configuration = serviceProvider.GetRequiredService<IConfiguration>();
            var logger = serviceProvider.GetRequiredService<ILogger<ProducerService<T>>>();

            return new ProducerService<T>(configuration, logger, producerName);
        });

        return services;
    }

    public static IServiceCollection AddEasyKafkaConsumer<T>(
        this IServiceCollection services,
        string consumerName,
        Action<Message<string, T>> onMessage)
    {
        services.AddHostedService<ConsumerService<T>>(serviceProvider =>
        {
            var configuration = serviceProvider.GetRequiredService<IConfiguration>();
            var logger = serviceProvider.GetRequiredService<ILogger<ConsumerService<T>>>();

            return new ConsumerService<T>(configuration, logger, consumerName, onMessage);
        });

        return services;
    }
}