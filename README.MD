 Let's see how Kafka streams behaves with ton of data
 
 ```
 ./confluent destroy
 ./confluent start kafka
 ./kafka-topics --zookeeper localhost:2181 --create --topic input --partitions 6 --replication-factor 1
 ./kafka-topics --zookeeper localhost:2181 --create --topic output --partitions 6 --replication-factor 1
 ./kafka-topics --zookeeper localhost:2181 --list
 mvn install
 java -classpath target/streams-*jar com.github.framiere.ProduceDummyEvents
 java -classpath target/streams-*jar com.github.framiere.Reduce
 ./kafka-console-consumer --bootstrap-server localhost:9092 --topic output
 ./kafka-console-consumer --bootstrap-server localhost:9092 --topic input
 ```
 
Live action on my laptop : https://asciinema.org/a/4CbYudc3FBGWM1cySp5hWjxFg

```
...
Processed 400,001 events (100,000 events/s)                                                                                                                                                                                                   
Processed 500,002 events (100,001 events/s)                                                                                                                                                                                                   
Processed 600,003 events (100,001 events/s)                                                                                                                                                                                                   
Processed 700,000 events (100,000 events/s)                                                                                                                                                                                                   
Processed 800,001 events (114,287 events/s)                                                                                                                                                                                                   
Processed 900,000 events (112,500 events/s)                                                                                                                                                                                                   
Processed 1,000,003 events (111,112 events/s)                                                                                                                                                                                                 
Processed 1,100,001 events (122,223 events/s)                                                                                                                                                                                                 
Processed 1,200,001 events (120,000 events/s)                                                                                                                                                                                                 
Processed 1,300,000 events (118,182 events/s)                                                                                                                                                                                                 
Processed 1,400,000 events (116,666 events/s)                                                                                                                                                                                                 
...
```
