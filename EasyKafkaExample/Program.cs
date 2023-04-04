using EasyKafka.Extensions;
using EasyKafka.Services;
using EasyKafkaExample.Data;
using EasyKafkaExample.Models;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Two ways to add the producer service
builder.Services.AddEasyKafkaProducer<Person>(producerName: "PersonProducer");
builder.Services.AddSingleton<ProducerService<string>>(provider =>
{
    var logger = provider.GetRequiredService<ILogger<ProducerService<string>>>();
    var configuration = provider.GetRequiredService<IConfiguration>();
    const string producerName = "TestProducer";
    
    return new ProducerService<string>(configuration, logger, producerName);
});

// Two ways to add the consumer service
builder.Services.AddEasyKafkaConsumer<Person>("PersonConsumer", ProcessData<Person>.OnMessageReceived);
builder.Services.AddHostedService<ConsumerService<string>>(provider =>
{
    var logger = provider.GetRequiredService<ILogger<ConsumerService<string>>>();
    var configuration = provider.GetRequiredService<IConfiguration>();
    const string consumerName = "TestConsumer";
    
    return new ConsumerService<string>(configuration, logger, consumerName, ProcessData<string>.OnMessageReceived);
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();