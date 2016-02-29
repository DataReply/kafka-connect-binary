# kafka-connect-binary
The connector is used to load data binary file to Kafka from a Directory.

# Building
You can build the connector with Maven using the standard lifecycle phases:
```
mvn clean
mvn package
```

# Sample Configuration
``` ini
name=local-binary-source
connector.class=org.apache.kafka.connect.binary.BinarySourceConnector
tasks.max=1
tmp.path=./tmp
check.dir.ms=1000
schema.name=filebinaryschema
topic=file-binary
```

# Configuration for Producer, Consumer and Broker
``` ini
producer.max.request.size = 20000000  #max size for your file
consumer.fetch.message.max.bytes = 20000000
broker.replica.fetch.max.bytes = 20000000
broker.message.max.bytes = 20000000
```

