using DIME.LoadTester;
using PowerArgs;

var config = Args.Parse<LoadTestConfig>(args);
var tester = new MqttLoadTester(config);
await tester.RunTest();