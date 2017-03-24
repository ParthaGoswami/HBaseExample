This Git project contains sorice code adopted from https://github.com/tomwhite/hadoop-book/

In this project data from HDFS/local file is exported to a HBase table and the data in a HTable is queried.

Steps & order to compile programs to load stations data into HTable

1. javac -cp $CLASSPATH:. NcdcStationMetadata.java
1. javac -cp $CLASSPATH:. NcdcStationMetadataParser.java
1. javac -cp $CLASSPATH:. HBaseStationQuery.java
1. javac -cp $CLASSPATH:. HBaseStationQuery.java

Importing Data into stations HTable
1. java -cp $CLASSPATH:. HBaseStationImporter hdfs hdfs://hadoop1:9000/CS5433/WeatherData/stations-fixed-width.txt tashwin:stations hdfs://hadoop1:9000
