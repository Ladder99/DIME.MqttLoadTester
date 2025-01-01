namespace DIME.LoadTester;

using MQTTnet;
using System.Diagnostics;
using System.Text.Json;

class MqttLoadTester
{
    private readonly LoadTestConfig _config;
    private readonly List<IMqttClient> _clients = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Metrics _metrics = new();
    private Timer _throughputTimer;
    private readonly SemaphoreSlim _connectionSemaphore = new(50); // Max 50 concurrent connections

    
    public MqttLoadTester(LoadTestConfig config)
    {
        _config = config;
    }

    public async Task RunTest()
    {
        try
        {
            Console.WriteLine($"Starting load test with {_config.NumberOfClients} clients into {_config.BrokerHost}");
            await CreateClients();
            StartThroughputReporting();
            await SimulateLoad();
            await DisplayResults();
        }
        finally
        {
            await Cleanup();
        }
    }

    private void StartThroughputReporting()
    {
        var lastMessageCount = 0L;
        _throughputTimer = new Timer(_ =>
        {
            var currentCount = _metrics.SuccessfulMessages;
            var throughput = currentCount - lastMessageCount;
            Console.WriteLine($"Throughput: {throughput} msgs/sec | Total: {currentCount:N0} | Failures: {_metrics.FailedMessages:N0}");
            lastMessageCount = currentCount;
        }, null, 0, 1000);
    }

    private async Task CreateClients()
    {
        var factory = new MqttClientFactory();
        var tasks = new List<Task>();
        
        for (int i = 0; i < _config.NumberOfClients; i++)
        {
            var clientId = i;
            tasks.Add(Task.Run(async () =>
            {
                await _connectionSemaphore.WaitAsync();
                try
                {
                    var client = factory.CreateMqttClient();
                    var builder = new MqttClientOptionsBuilder()
                        .WithTcpServer(_config.BrokerHost, _config.BrokerPort)
                        .WithClientId($"loadtest_client_{Guid.NewGuid().ToString()}_{clientId}");
                    
                    if (_config.CleanSession)
                        builder.WithCleanSession();

                    if (!string.IsNullOrEmpty(_config.Username) && !string.IsNullOrEmpty(_config.Password))
                        builder.WithCredentials(_config.Username, _config.Password);
                    
                    var options = builder.Build();

                    await client.ConnectAsync(options);
                    lock (_clients)
                    {
                        _clients.Add(client);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to connect client {clientId}: {ex.Message}");
                }
                finally
                {
                    _connectionSemaphore.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);
        Console.WriteLine($"Connected {_clients.Count} clients");
    }

    private async Task SimulateLoad()
    {
        var sw = Stopwatch.StartNew();
        var tasks = new List<Task>();

        for (int i = 0; i < _config.NumberOfMessages; i++)
        {
            foreach (var client in _clients)
            {
                var message = new { timestamp = DateTime.UtcNow, value = Random.Shared.Next(100) };
                var payload = JsonSerializer.Serialize(message);

                tasks.Add(PublishMessageAsync(client, _config.Topic, payload));
            }

            if (tasks.Count >= 1000)
            {
                await Task.WhenAll(tasks);
                tasks.Clear();
                
                if (_config.MessageDelayMs > 0)
                    await Task.Delay(_config.MessageDelayMs);
            }
        }

        await Task.WhenAll(tasks);
        sw.Stop();
        _metrics.TotalDuration = sw.Elapsed;
    }

    private async Task PublishMessageAsync(IMqttClient client, string topic, string payload)
    {
        try
        {
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel((MQTTnet.Protocol.MqttQualityOfServiceLevel)_config.QoS)
                .Build();

            var sw = Stopwatch.StartNew();
            await client.PublishAsync(message);
            sw.Stop();

            Interlocked.Increment(ref _metrics.SuccessfulMessages);
            _metrics.AddLatency(sw.ElapsedMilliseconds);
        }
        catch (Exception)
        {
            Interlocked.Increment(ref _metrics.FailedMessages);
        }
    }

    private async Task DisplayResults()
    {
        _throughputTimer?.Dispose();
        
        var totalMessages = _metrics.SuccessfulMessages + _metrics.FailedMessages;
        var messagesPerSecond = totalMessages / _metrics.TotalDuration.TotalSeconds;

        Console.WriteLine($"\nLoad Test Results:");
        Console.WriteLine($"Total Messages: {totalMessages:N0}");
        Console.WriteLine($"Successful: {_metrics.SuccessfulMessages:N0}");
        Console.WriteLine($"Failed: {_metrics.FailedMessages:N0}");
        Console.WriteLine($"Average Latency: {_metrics.AverageLatency:N2}ms");
        Console.WriteLine($"Messages/sec: {messagesPerSecond:N2}");
        Console.WriteLine($"Total Duration: {_metrics.TotalDuration.TotalSeconds:N2}s");
    }

    private async Task Cleanup()
    {
        _throughputTimer?.Dispose();
        foreach (var client in _clients)
        {
            try
            {
                await client.DisconnectAsync();
                client.Dispose();
            }
            catch { }
        }
        _cts.Dispose();
    }
}

class LoadTestConfig
{
    public string BrokerHost { get; set; } = "localhost";
    public int BrokerPort { get; set; } = 1883;
    public int NumberOfClients { get; set; } = 100;
    public int NumberOfMessages { get; set; } = 1000;
    public int MessageDelayMs { get; set; } = 10;
    public string Topic { get; set; } = "loadtest";
    public int QoS { get; set; } = 1;
    public bool CleanSession { get; set; } = true;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
}

class Metrics
{
    public long SuccessfulMessages;
    public long FailedMessages;
    private readonly List<long> _latencies = new();
    public TimeSpan TotalDuration;

    public void AddLatency(long latencyMs)
    {
        lock (_latencies)
        {
            _latencies.Add(latencyMs);
        }
    }

    public double AverageLatency
    {
        get
        {
            lock (_latencies)
            {
                return _latencies.Count > 0 ? _latencies.Average() : 0;
            }
        }
    }
}
