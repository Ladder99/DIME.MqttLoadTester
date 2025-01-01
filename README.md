# DIME.MqttLoadTester
MQTT Load Test

```
DIME.MqttLoadTester.exe ^
  -brokerhost 127.0.0.1 ^
  -brokerport 1883 ^
  -numberofclients 5 ^
  -numberofmessages 1000000000 ^
  -messagedelayms 1 ^
  -topic loadtest ^
  -qos 0 ^
  -cleansession true ^
  -username user ^
  -password password
```
