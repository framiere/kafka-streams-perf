 Let's see how Kafka streams beahves with ton of data
 
 ```
 ./confluent destroy
 ./confluent start kafka
 ./kafka-topics --zookeeper localhost:2181 --create --topic input --partitions 6 --replication-factor 1
 ./kafka-topics --zookeeper localhost:2181 --create --topic output --partitions 6 --replication-factor 1
 ./kafka-topics --zookeeper localhost:2181 --list
 ./mvn install
 java -classpath target/streams-*jar com.github.framiere.ProduceDummyEvents
 java -classpath target/streams-*jar com.github.framiere.Reduce
 ./kafka-console-consumer --bootstrap-server localhost:9092 --topic output
 ./kafka-console-consumer --bootstrap-server localhost:9092 --topic input
 ```
 
Live action : https://asciinema.org/a/4CbYudc3FBGWM1cySp5hWjxFg
