using EasyKafka.Services;
using Microsoft.AspNetCore.Mvc;

namespace EasyKafkaExample.Controllers;

[ApiController]
[Route("[controller]")]
public class TestController : ControllerBase
{
    private readonly ILogger<TestController> _logger;
    private readonly ProducerService<string> _producerService;
    
    public TestController(ILogger<TestController> logger, ProducerService<string> producerService)
    {
        _logger = logger;
        _producerService = producerService;
    }

    [HttpGet(Name = "GetTest")]
    public async Task<ActionResult> Get()
    {
        _producerService.ProduceAsync("testKey", "testValue");
        
        return Ok();
    }
}