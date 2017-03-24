package edu.okstate.cs.tashwin.Temperature;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*Source code adopted from URL
 * https://github.com/tomwhite/hadoop-book/blob/master/ch20-hbase/src/main/java/HBaseTemperatureImporter.java
 * Addition made are as follows
 * 1. Allowing the users to specify the table name to import contents to*/

public class HBaseTemperatureImporter extends Configured implements Tool {
  
  static class HBaseTemperatureMapper<K> extends Mapper<LongWritable, Text, K, Put> {
    private NcdcRecordParser parser = new NcdcRecordParser();

    @SuppressWarnings("deprecation")
	@Override
    public void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {
      parser.parse(value.toString());
      if (parser.isValidTemperature()) {
        byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
            parser.getObservationDate().getTime());
        Put p = new Put(rowKey);
        p.add(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
            HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
            Bytes.toBytes(parser.getAirTemperature()));
        context.write(null, p);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: HBaseTemperatureImporter <input> <table-name>");
      return -1;
    }
    String inputPath = args[0];
    String hbaseTable = args[1];
    
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
    @SuppressWarnings("deprecation")
	Job job = new Job(getConf(), getClass().getSimpleName());
    job.setJarByClass(getClass());
    FileInputFormat.addInputPath(job, new Path(inputPath));
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hbaseTable);
    job.setMapperClass(HBaseTemperatureMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(TableOutputFormat.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseTemperatureImporter(), args);
    System.exit(exitCode);
  }
}