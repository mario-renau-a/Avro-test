To run the tests, you should run sbt test. 
In case you are doing improvements that target speed, you can generate a sample Avro file and check how long it takes to read that Avro file using the following commands:

```sbtshell
sbt "test:runMain com.mastria.spark.avro.AvroFileGenerator 10000 2"
```
This will create sample avro files in target/avroForBenchmark/. You can specify the number of records for each file, as well as the overall number of files.

```sbtshell
sbt "test:runMain com.mastria.spark.avro.AvroReadBenchmark"
```
runs count() on the data inside target/avroForBenchmark/ and tells you how the operation took.

Similarly, you can do benchmarks on how long it takes to write DataFrame as Avro file with

```sbtshell
sbt "test:runMain com.mastria.spark.avro.AvroWriteBenchmark NUMBER_OF_ROWS"
```
where NUMBER_OF_ROWS is an optional parameter that allows you to specify the number of rows in DataFrame that we will be writing.




# Schema Registry
Download:
https://github.com/renukaradhya/confluentplatform
### Start Zookeeper
```sh
.\zookeeper-server-start.bat ..\..\etc\kafka\zookeeper.properties
 ```
### Start Kafka Server
```sh
.\kafka-server-start.bat ..\..\etc\kafka\server.properties
```
### Start Schema Registry
```sh
.\schema-registry-start.bat ..\..\etc\schema-registry\schema-registry.properties
```