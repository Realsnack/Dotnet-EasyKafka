using EasyKafka.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;

namespace EasyKafkaTests;

public class ProducerServiceTests
{
    private readonly Mock<IConfiguration> _configuration;
    private readonly Mock<ILogger<ProducerService<string>>> _logger;
    private const string producerName = "TestProducer";
    
    public ProducerServiceTests()
    {
        _configuration = new Mock<IConfiguration>();
        var kafkaConfigurationSection = new Mock<IConfigurationSection>();

        kafkaConfigurationSection.Setup(x => x["BootstrapServers"]).Returns("localhost:9092");
        kafkaConfigurationSection.Setup(x => x["SchemaRegistryUrl"]).Returns("http://localhost:8081");
        _configuration.Setup(x => x[$"Kafka:Producer:{producerName}:Topic"]).Returns("TestTopic");
        _configuration.Setup(x => x.GetSection($"Kafka:Producer:{producerName}"))
            .Returns(kafkaConfigurationSection.Object);

        _logger = new Mock<ILogger<ProducerService<string>>>();
    }

    #region Constructor

    [Fact]
    public void Constructor_WhenCalled_SetsProperties()
    {
        // Act
        var producerService = new ProducerService<string>(_configuration.Object, _logger.Object, producerName);

        // Assert
        Assert.Equal(producerName, producerService.ProducerName);
        Assert.Equal("TestTopic", producerService.Topic);
    }

    [Fact]
    public void Constructor_WhenCalled_EmptyTopicThrowsException()
    {
        // Mock IConfiguration
        var configuration = new Mock<IConfiguration>();
        var kafkaConfigurationSection = new Mock<IConfigurationSection>();

        // Setup configuration section
        kafkaConfigurationSection.Setup(x => x["BootstrapServers"]).Returns("localhost:9092");
        kafkaConfigurationSection.Setup(x => x["SchemaRegistryUrl"]).Returns("http://localhost:8081");
        configuration.Setup(x => x.GetSection($"Kafka:Producer:{producerName}"))
            .Returns(kafkaConfigurationSection.Object);

        var logger = new Mock<ILogger<ProducerService<string>>>();

        // Act && Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ProducerService<string>(configuration.Object, _logger.Object, producerName));
    }

    [Fact]
    public void Constructor_WhenCalled_EmptyBootstrapServersThrowsException()
    {
        // Arrange
        var configuration = new Mock<IConfiguration>();
        var kafkaConfigurationSection = new Mock<IConfigurationSection>();
        
        // Setup configuration section
        kafkaConfigurationSection.Setup(x => x["SchemaRegistryUrl"]).Returns("http://localhost:8081");
        configuration.Setup(x => x[$"Kafka:Producer:{producerName}:Topic"]).Returns("TestTopic");
        configuration.Setup(x => x.GetSection($"Kafka:Producer:{producerName}"))
            .Returns(kafkaConfigurationSection.Object);

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ProducerService<string>(configuration.Object, _logger.Object, producerName));
    }

    [Fact]
    public void Constructor_WhenCalled_EmptySchemaRegistryUrlThrowsException()
    {
        // Arrange
        var configuration = new Mock<IConfiguration>();
        var kafkaConfigurationSection = new Mock<IConfigurationSection>();
        
        // Setup configuration section
        kafkaConfigurationSection.Setup(x => x["BootstrapServers"]).Returns("localhost:9092");
        configuration.Setup(x => x[$"Kafka:Producer:{producerName}:Topic"]).Returns("TestTopic");
        configuration.Setup(x => x.GetSection($"Kafka:Producer:{producerName}"))
            .Returns(kafkaConfigurationSection.Object);
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ProducerService<string>(configuration.Object, _logger.Object, producerName));
    }

    #endregion

    #region LoadConfiguration

    [Fact]
    public void LoadConfiguration_WhenCalled_SetsProperties()
    {
        // Arrange
        var producerService = new ProducerService<string>(_configuration.Object, _logger.Object, producerName);
        
        // Clear properties
        producerService.Topic = null;
        producerService.Config = null;
        producerService.SchemaRegistryClient = null;
        
        // Act
        producerService.LoadConfiguration(_configuration.Object, producerName);
        
        // Assert
        Assert.NotNull(producerService.Topic);
        Assert.NotNull(producerService.Config);
        Assert.NotNull(producerService.SchemaRegistryClient);
        Assert.Equal("TestTopic", producerService.Topic);
        Assert.Equal("localhost:9092", producerService.Config.BootstrapServers);
    }
    #endregion
}