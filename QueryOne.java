import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class QueryOne{

public static final String TABLE = "AIRINFO" ;

   public static void main(String args[]) throws IOException{


      Configuration config = HBaseConfiguration.create();
      HTable table = new HTable(config, TABLE);

      // instantiate the Scan class
      Scan scan = new Scan();

      // scan the columns
      scan.addColumn(Bytes.toBytes("origin"), Bytes.toBytes("city"));
   //   scan.addColumn(Bytes.toBytes("origin"), Bytes.toBytes("state"));  // along with state


      Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new
      SubstringComparator(args[0]));
      FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE,filter1);
      scan.setFilter(list);


      // get the ResultScanner
      ResultScanner scanner = table.getScanner(scan);
      for (Result result = scanner.next(); result != null; result=scanner.next())
        System.out.println("Found row : " + result);

      scanner.close();
   }
}
