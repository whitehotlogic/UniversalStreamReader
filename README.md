# UniversalStreamReader
___

This solution was built using the following requirements:

1) Should be able to read from Apache Kafka, but  design should allow to add more reader implementations (for example for Azure EventHub).

2) Should have configurable in-memory cache with:

*  Invalidation by time and/or by message count (ring buffer)

*  Persistence to file and to database engine (pick your favorite). Design should allow to add more persistent storage implementations

3) User interface 

4) Tests (Unit tests / Integration test)

___

This repository contains the following components:

* KafkaProducer 

* UniversalStreamReader 

* Unit Tests 

* Integration Tests 

> **Note:** All dependencies have been removed from this repository (using .gitignore) so make sure to refresh your NuGet packages after importing the project to Visual Studio.

### Instructions:

NOTE: Requires functional Kafka instance.
1) In the code: change the DBPersist, FilePersist, Tests, paths to match your local storage environment
2) In the code: change the Kafka server IP address to match your Kafka configuration
3) In the code: change the ringbuffer size to your preference
4) Build the solution in Visual Studio (make sure NuGet packages are reinstalled)
5) Run KafkaProducer.exe
6) Run UniversalStreamReader.exe
7) Type a message into the producer, see it output on the consumer
8) Check your .csv file to see the data
9) Use a sqlite db browser to check your .sqlite file to see the data
10) Profit