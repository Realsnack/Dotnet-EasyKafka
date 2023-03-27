using EasyKafka.Extensions;
using EasyKafka.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Two ways to add the producer service
builder.Services.AddEasyKafkaProducer<string>(producerName: "OtherProducer");
builder.Services.AddSingleton<ProducerService<string>>(provider =>
{
    var logger = provider.GetRequiredService<ILogger<ProducerService<string>>>();
    var configuration = provider.GetRequiredService<IConfiguration>();
    var producerName = "TestProducer";
    
    return new ProducerService<string>(configuration, logger, producerName);
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