using EasyKafka.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
// using Moq;
using NSubstitute;
using Xunit.Abstractions;

namespace EasyKafkaTests;

public class ProducerServiceTests
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<ProducerService<string>> _logger;
    private const string producerName = "TestProducer";

    public ProducerServiceTests()
    {
        _configuration = Substitute.For<IConfiguration>();
        var kafkaConfigurationSection = Substitute.For<IConfigurationSection>();

        kafkaConfigurationSection["BootstrapServers"].Returns("localhost:9092");
        kafkaConfigurationSection["SchemaRegistryUrl"].Returns("http://localhost:8081");
        _configuration[$"Kafka:Producer:{producerName}:Topic"].Returns("TestTopic");
        _configuration.GetSection($"Kafka:Producer:{producerName}")
            .Returns(kafkaConfigurationSection);

        _logger = Substitute.For<ILogger<ProducerService<string>>>();
    }

    #region Constructor

    [Fact]
    public void Constructor_WhenCalled_SetsProperties()
    {
        // Act
        var producerService = new ProducerService<string>(_configuration, _logger, producerName);

        // Assert
        Assert.Equal(producerName, producerService.ProducerName);
        Assert.Equal("TestTopic", producerService.Topic);
    }

    [Fact]
    public void Constructor_WhenCalled_NullTopicThrowsException()
    {
        // Mock IConfiguration
        var configuration = Substitute.For<IConfiguration>();
        var kafkaConfigurationSection = Substitute.For<IConfigurationSection>();

        // Setup configuration section
        kafkaConfigurationSection["BootstrapServers"].Returns("localhost:9092");
        kafkaConfigurationSection["SchemaRegistryUrl"].Returns("http://localhost:8081");
        configuration[$"Kafka:Producer:{producerName}:Topic"].Returns((string?)null);
        configuration.GetSection($"Kafka:Producer:{producerName}")
            .Returns(kafkaConfigurationSection);

        var logger = Substitute.For<ILogger<ProducerService<string>>>();

        // Act && Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ProducerService<string>(configuration, _logger, producerName));
    }

    [Fact]
    public void Constructor_WhenCalled_NullBootstrapServersThrowsException()
    {
        // Arrange
        var configuration = Substitute.For<IConfiguration>();
        var kafkaConfigurationSection = Substitute.For<IConfigurationSection>();

        // Setup configuration section
        kafkaConfigurationSection["BootstrapServers"].Returns((string?)null);
        kafkaConfigurationSection["SchemaRegistryUrl"].Returns("http://localhost:8081");
        configuration[$"Kafka:Producer:{producerName}:Topic"].Returns("TestTopic");
        configuration.GetSection($"Kafka:Producer:{producerName}")
            .Returns(kafkaConfigurationSection);

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ProducerService<string>(configuration, _logger, producerName));
    }

    [Fact]
    public void Constructor_WhenCalled_NullSchemaRegistryUrlThrowsException()
    {
        // Arrange
        var configuration = Substitute.For<IConfiguration>();
        var kafkaConfigurationSection = Substitute.For<IConfigurationSection>();

        // Setup configuration section
        kafkaConfigurationSection["BootstrapServers"].Returns("localhost:9092");
        kafkaConfigurationSection["SchemaRegistryUrl"].Returns((string?)null);
        configuration[$"Kafka:Producer:{producerName}:Topic"].Returns("TestTopic");
        configuration.GetSection($"Kafka:Producer:{producerName}")
            .Returns(kafkaConfigurationSection);
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ProducerService<string>(configuration, _logger, producerName));
    }
    #endregion

    #region LoadConfiguration
    [Fact]
    public void LoadConfiguration_WhenCalled_SetsProperties()
    {
        // Arrange
        var producerService = new ProducerService<string>(_configuration, _logger, producerName);
        
        // Clear properties
        producerService.Topic = null;
        producerService.Config = null;
        producerService.SchemaRegistryClient = null;
        
        // Act
        producerService.LoadConfiguration(_configuration, producerName);
        
        // Assert
        Assert.NotNull(producerService.Topic);
        Assert.NotNull(producerService.Config);
        Assert.NotNull(producerService.SchemaRegistryClient);
        Assert.Equal("TestTopic", producerService.Topic);
        Assert.Equal("localhost:9092", producerService.Config.BootstrapServers);
    }
    #endregion
}
