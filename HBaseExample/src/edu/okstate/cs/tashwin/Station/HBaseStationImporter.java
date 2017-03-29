/*Source code adopted from URL
 * https://github.com/tomwhite/hadoop-book/blob/master/ch20-hbase/src/main/java/NewHBaseStationImporter.java
 * Additions made are as follows
 * 1. Enabling the program to read from HDFS along with local FS
 * 2. Allowing the users to specify the table name to import contents to
 * 3. Allowing the users to specify a name node URL if they have to import data from HDFS*/


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase 1.0 version of HBaseStationImporter that uses {@code Connection},
 * and {@code Table}.
 */
public class HBaseStationImporter extends Configured implements Tool {
  
  @SuppressWarnings({ "deprecation" })
public int run(String[] args) throws IOException {
	 /*Check for the arguments*/
    if (args.length < 3 || args.length > 5) {
      System.err.println("Usage: HBaseStationImporter <file-system> <input> <table-name> <hdfs-master-url>");
      System.err.println(args.length);
      return -1;
    }
    
    String fileSystem = args[0];
    String inputPath = args[1];
    String hbaseTable = args[2];
    boolean isHdfsPath = args.length ==4? true:false ;
    String hdfsMaster = null;
    
    /*Check whether file system is null/empty */
    if (fileSystem.length() == 0 || fileSystem == null){
    	System.err.println("File System cannot be null/empty");
        return -1;
    }else if (fileSystem.equalsIgnoreCase("local") || fileSystem.equalsIgnoreCase("hdfs")){
    	System.out.println(fileSystem.toUpperCase() + " File system is supported");
    }else{
    	System.err.println("Only Local/HDFS file system is supported"+fileSystem);
        return -1;
    }
    /*Check whether input path is null/empty */
    if (inputPath.length() == 0 || inputPath == null){
    	System.err.println("Input Path cannot be null/empty");
        return -1;
    }
    /*Check whether HTable name is null/empty */
    if (hbaseTable.length() == 0 || hbaseTable == null){
    	System.err.println("HTable name cannot be null/empty");
        return -1;
    }
    /*If file is being read from HDFS check whether HDFS Master URL name is null/empty */
    if (isHdfsPath){
    	if(args[3].length() !=0 && args[3] != null){
    		hdfsMaster = args[3];
    	}else{
    		System.err.println("HDFS Master URL cannot be null/empty");
            return -1;
    	}
    }
    /*System.out.println(args[0]+args[1]+args[2]+isHdfsPath+hdfsMaster);
    System.exit(1);*/
    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    try {
      /*Get an object for the table we created*/
      TableName tableName = TableName.valueOf(hbaseTable);
      Table table = connection.getTable(tableName);
      try {
        NcdcStationMetadata metadata = new NcdcStationMetadata();
        metadata.initialize(inputPath, isHdfsPath, hdfsMaster);
        Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();

        for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
          Put put = new Put(Bytes.toBytes(entry.getKey()));
          put.add(HBaseStationQuery.INFO_COLUMNFAMILY,
        		  HBaseStationQuery.NAME_QUALIFIER, Bytes.toBytes(entry.getValue()));
          put.add(HBaseStationQuery.INFO_COLUMNFAMILY,
              HBaseStationQuery.DESCRIPTION_QUALIFIER, Bytes.toBytes("(unknown)"));
          put.add(HBaseStationQuery.INFO_COLUMNFAMILY,
              HBaseStationQuery.LOCATION_QUALIFIER, Bytes.toBytes("(unknown)"));
          table.put(put);
        }
      } finally {
        table.close();
      }
    } finally {
      connection.close();
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseStationImporter(), args);
    System.exit(exitCode);
  }
}
