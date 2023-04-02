using System.Linq.Expressions;
using EasyKafka.Services;
using EasyKafkaExample.Models;
using Microsoft.AspNetCore.Mvc;

namespace EasyKafkaExample.Controllers;

[ApiController]
[Route("[controller]")]
public class TestController : ControllerBase
{
    private readonly ILogger<TestController> _logger;
    private readonly ProducerService<string> _producerService;
    private readonly ProducerService<Person> _personProducerService;

    public TestController(ILogger<TestController> logger, ProducerService<string> producerService,
        ProducerService<Person> personProducerService)
    {
        _logger = logger;
        _producerService = producerService;
        _personProducerService = personProducerService;
    }

    [HttpGet(Name = "GetTest")]
    public async Task<ActionResult> Get()
    {
        _producerService.ProduceAsync("testKey", "testValue");

        return Ok();
    }

    [HttpGet("Person", Name = "GetPerson")]
    public async Task<ActionResult> GetPerson()
    {
        var person = new Person
        {
            Age = 69,
            FirstName = "John",
            LastName = "Doe"
        };

        _personProducerService.ProduceAsync("testKey", person);

        return Ok();
    }
}