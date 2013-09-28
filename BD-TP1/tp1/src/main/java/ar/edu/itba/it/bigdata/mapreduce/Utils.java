package ar.edu.itba.it.bigdata.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Utils {
	
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
	
	public static HTable getTable(Context context, String tableName) {
		Configuration hConf = HBaseConfiguration.create();
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "hadoop-2013-datanode-1");
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, "2181");
		try {
			return new HTable(hConf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static Scan getScan(String family) {
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		scan.addFamily(family.getBytes());
		return scan;
	}
	
	public static BufferedReader getBufferedReader(Context context) {
		Path[] paths = DistributedCache.getArchiveClassPaths(context.getConfiguration());
		FileSystem fs = null;
		FSDataInputStream inputStream = null;
		try {
			fs = FileSystem.get(context.getConfiguration());
			inputStream = fs
					.open(paths[0]);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// Read the broadcasted file
		BufferedReader br = new BufferedReader(new InputStreamReader(
				inputStream));
		
		return br;
	}

}
