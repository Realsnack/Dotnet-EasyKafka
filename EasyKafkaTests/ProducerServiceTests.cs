using Confluent.Kafka;
using EasyKafka.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;

namespace EasyKafkaTests;

public class ProducerServiceTests
{
    #region Constructor
    [Fact]
    public void Constructor_WhenCalled_SetsProperties()
    {
        // Arrange
        const string producerName = "TestProducer";
        
        // Mock IConfiguration
        var configuration = new Mock<IConfiguration>();
        var kafkaConfigurationSection = new Mock<IConfigurationSection>();
        var schemaRegistryConfigurationSection = new Mock<IConfigurationSection>();
        
        // Setup configuration section
        kafkaConfigurationSection.Setup(x => x["BootstrapServers"]).Returns("localhost:9092");
        kafkaConfigurationSection.Setup(x => x["SchemaRegistryUrl"]).Returns("http://localhost:8081");
        configuration.Setup(x => x[$"Kafka:Producer:{producerName}:Topic"]).Returns("TestTopic");
        configuration.Setup(x => x.GetSection($"Kafka:Producer:{producerName}")).Returns(kafkaConfigurationSection.Object);

        var logger = new Mock<ILogger<ProducerService<string>>>();
        
        // Act
        var producerService = new ProducerService<string>(configuration.Object, logger.Object, producerName);
        
        // Assert
        Assert.Equal(producerName, producerService._producerName);
        Assert.Equal("TestTopic", producerService._topic);
    }
    
    [Fact]
    public void Constructor_WhenCalled_EmptyTopicThrowsException()
    {
        // Arrange
        const string producerName = "TestProducer";
        
        // Mock IConfiguration
        var configuration = new Mock<IConfiguration>();
        var kafkaConfigurationSection = new Mock<IConfigurationSection>();
        var schemaRegistryConfigurationSection = new Mock<IConfigurationSection>();
        
        // Setup configuration section
        kafkaConfigurationSection.Setup(x => x["BootstrapServers"]).Returns("localhost:9092");
        kafkaConfigurationSection.Setup(x => x["SchemaRegistryUrl"]).Returns("http://localhost:8081");
        configuration.Setup(x => x.GetSection($"Kafka:Producer:{producerName}")).Returns(kafkaConfigurationSection.Object);

        var logger = new Mock<ILogger<ProducerService<string>>>();
        
        // Act && Assert
        Assert.Throws<ArgumentNullException>(() => new ProducerService<string>(configuration.Object, logger.Object, producerName));
    }
    
    [Fact]
    public void Constructor_WhenCalled_EmptyBootstrapServersThrowsException()
    {
        // Arrange
        const string producerName = "TestProducer";
        
        // Mock IConfiguration
        var configuration = new Mock<IConfiguration>();
        var kafkaConfigurationSection = new Mock<IConfigurationSection>();
        var schemaRegistryConfigurationSection = new Mock<IConfigurationSection>();
        
        // Setup configuration section
        kafkaConfigurationSection.Setup(x => x["SchemaRegistryUrl"]).Returns("http://localhost:8081");
        configuration.Setup(x => x[$"Kafka:Producer:{producerName}:Topic"]).Returns("TestTopic");
        configuration.Setup(x => x.GetSection($"Kafka:Producer:{producerName}")).Returns(kafkaConfigurationSection.Object);

        var logger = new Mock<ILogger<ProducerService<string>>>();
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new ProducerService<string>(configuration.Object, logger.Object, producerName));
    }
    
    [Fact]
    public void Constructor_WhenCalled_EmptySchemaRegistryUrlThrowsException()
    {
        // Arrange
        const string producerName = "TestProducer";
        
        // Mock IConfiguration
        var configuration = new Mock<IConfiguration>();
        var kafkaConfigurationSection = new Mock<IConfigurationSection>();
        var schemaRegistryConfigurationSection = new Mock<IConfigurationSection>();
        
        // Setup configuration section
        kafkaConfigurationSection.Setup(x => x["BootstrapServers"]).Returns("localhost:9092");
        configuration.Setup(x => x[$"Kafka:Producer:{producerName}:Topic"]).Returns("TestTopic");
        configuration.Setup(x => x.GetSection($"Kafka:Producer:{producerName}")).Returns(kafkaConfigurationSection.Object);

        var logger = new Mock<ILogger<ProducerService<string>>>();
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new ProducerService<string>(configuration.Object, logger.Object, producerName));
    }
    #endregion
}
