package edu.okstate.cs.tashwin.Station;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class NcdcStationMetadata {
  
  private Map<String, String> stationIdToName = new HashMap<String, String>();

  public void initialize(String file_path, boolean isHdfsPath, String HdfsMasterURL) throws IOException {
	  BufferedReader in = null;
	  try {
		  if (isHdfsPath){
			  Path pt=new Path(file_path);
			  Configuration config = new Configuration();
		      config.set("fs.defaultFS", HdfsMasterURL);
              FileSystem fs = FileSystem.get(config);
              in=new BufferedReader(new InputStreamReader(fs.open(pt)));
		  }else{
			  in = new BufferedReader(new InputStreamReader(new FileInputStream(new File(file_path))));
		  }
		  NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
		  String line;
		  while ((line = in.readLine()) != null) {
			  if (parser.parse(line)) {
				  stationIdToName.put(parser.getStationId(), parser.getStationName());
			  }
		  }
	  } finally {
		  IOUtils.closeStream(in);
	  }
  }

  public String getStationName(String stationId) {
    String stationName = stationIdToName.get(stationId);
    if (stationName == null || stationName.trim().length() == 0) {
      return stationId; // no match: fall back to ID
    }
    return stationName;
  }
  
  public Map<String, String> getStationIdToNameMap() {
    return Collections.unmodifiableMap(stationIdToName);
  }
  
}
