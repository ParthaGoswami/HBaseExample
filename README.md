This Git project contains sorice code adopted from https://github.com/tomwhite/hadoop-book/

In this project data from HDFS/local file is exported to a HBase table and the data in a HTable is queried.

Create two HTables for stations & observations by issuing following commands in HBase shell client
1. create 'namespace:stations', {NAME => 'info'} 
1. create 'namespace:observations', {NAME => 'data’}


Steps & order to compile programs to load stations data into HTable

1. javac -cp $CLASSPATH:. NcdcStationMetadata.java
1. javac -cp $CLASSPATH:. NcdcStationMetadataParser.java
1. javac -cp $CLASSPATH:. HBaseStationQuery.java
1. javac -cp $CLASSPATH:. HBaseStationQuery.java

Importing Data into stations HTable
1. java -cp $CLASSPATH:. HBaseStationImporter hdfs path/to/stations-fixed-width.txt namespace:stations hdfs://NameNode:9000

Open Hbase Shell and check if the rows have been inserted by typing "count 'tashwin:stations'" on the cLI. If data has been insterted the count should not be zero

Steps & order to compile programs to load observations data into HTable

1. javac -cp $CLASSPATH:. RowKeyConverter.java
1. javac -cp $CLASSPATH:. NcdcRecordParser.java
1. javac -cp $CLASSPATH:. HBaseTemperatureQuery.java
1. javac -cp $CLASSPATH:. HBaseTemperatureImporter.java

Bundle all the java files into a JAR file

jar cvf MyExample.jar *

To import temperature data issue the following command in CLI


HADOOP_CLASSPATH=$(hbase classpath) hadoop jar MyExample.jar HBaseTemperatureImporter path/to/Observations namespace:observations

To Query Temperature & Stations use the following commands
1. hbase -cp $CLASSPATH:. HBaseTemperatureQuery namespace:observations 011990-99999
1. hbase -cp $CLASSPATH:. HBaseStationQuery namespace:stations 011990-99999
