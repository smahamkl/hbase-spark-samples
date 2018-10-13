package com.javacodegeeks.examples;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * 
 * This is a simple example of putting records in HBase
 * 
 * with the bulkPut function.
 * 
 */

@InterfaceAudience.Private

final public class JavaHBaseBulkPutExample {

	private JavaHBaseBulkPutExample() {
	}

	public static void main(String[] args) {

		/*
		 * if (args.length < 2) {
		 * 
		 * System.out.println("JavaHBaseBulkPutExample  " +
		 * 
		 * "{tableName} {columnFamily}");
		 * 
		 * return;
		 * 
		 * }
		 */

		String tableName = "JavaHBaseBulkPutExample";
		String columnFamily = "cf1";
		SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkPutExample " + tableName).setMaster("local[2]");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		try {

			List<String> list = new ArrayList<>(5);
			list.add("1," + columnFamily + ",a,1");
			list.add("2," + columnFamily + ",a,2");
			list.add("3," + columnFamily + ",a,3");
			list.add("4," + columnFamily + ",a,4");
			list.add("5," + columnFamily + ",a,5");

			JavaRDD<String> rdd = jsc.parallelize(list);
			Configuration conf = HBaseConfiguration.create();
			String path = ImportData.class.getClassLoader().getResource("hbase-site.xml").getPath();
			conf.addResource(new Path(path));

			JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

			hbaseContext.bulkPut(rdd, TableName.valueOf(tableName), new PutFunction());

		} finally {

			jsc.stop();

		}

	}

	public static class PutFunction implements Function<String, Put> {

		private static final long serialVersionUID = 1L;

		public Put call(String v) throws Exception {
			String[] cells = v.split(",");
			Put put = new Put(Bytes.toBytes(cells[0]));
			put.addColumn(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]), Bytes.toBytes(cells[3]));
			return put;

		}

	}

}
