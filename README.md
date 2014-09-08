##Simple Spark Examples In Scala
This sample codes demonstrate Spark Streaming, Spark Sql, Word Count

##Spark Streaming
Random data are ingested via mqtt to Spark. Spark transforms received data to char array to find character occurance.
1. The result is sent to browser via websocket. 
2. The data ingestion code is under ingest module.
3. The web socket code in under servlet module.
4. Streaming code is under sparkApp module.

Please install any mqtt broker.
#Mosquitto Broker Installation
% sudo apt-get install mosquitto
#Compile and make jar.
% mvn package

