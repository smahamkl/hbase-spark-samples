package com.javacodegeeks.examples;

import java.util.ArrayList;

import java.util.HashMap;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.spark.FamilyHFileWriteOptions;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * 
 * https://github.com/apache/hbase/tree/master/hbase-spark/src/main/java/org/apache/hadoop/hbase/spark/example/hbasecontext
 * Run this example using command below:
 * 
 * SPARK_HOME/bin/spark-submit --master local[2]
 * 
 * --class
 * org.apache.hadoop.hbase.spark.example.hbasecontext.JavaHBaseBulkLoadExample path/to/hbase-spark.jar {path/to/output/HFiles}
 *
 * This example will output put hfiles in {path/to/output/HFiles}, and user can
 * run 'hbase org.apache.hadoop.hbase.tool.LoadIncrementalHFiles' to load the HFiles
 * into table to verify this example.
 * 
 */

@InterfaceAudience.Private

final public class JavaHBaseBulkLoadExample {

	private JavaHBaseBulkLoadExample() {
	}

	public static void main(String[] args) {

/*		if (args.length < 1) {
			System.out.println("JavaHBaseBulkLoadExample  " + "{outputPath}");
			return;

		}*/

		String tableName = "bulkload-table-test";
		String columnFamily1 = "f1";
		String columnFamily2 = "f2";

		SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkLoadExample " + tableName).setMaster("local[2]");;
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		try {

			List<String> list = new ArrayList<String>();
			// row1
			list.add("1," + columnFamily1 + ",b,1");
			// row3
			list.add("3," + columnFamily1 + ",a,2");
			list.add("3," + columnFamily1 + ",b,1");
			list.add("3," + columnFamily2 + ",a,1");

			/* row2 */

			list.add("2," + columnFamily2 + ",a,3");
			list.add("2," + columnFamily2 + ",b,3");

			JavaRDD<String> rdd = jsc.parallelize(list);

			Configuration conf = HBaseConfiguration.create();
			
			String path = ImportData.class.getClassLoader().getResource("hbase-site.xml").getPath();
			conf.addResource(new Path(path));
			
			JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

			hbaseContext.bulkLoad(rdd, TableName.valueOf(tableName), new BulkLoadFunction(), "/home/sivam/d/data_repo/hbase_out",
					new HashMap<byte[], FamilyHFileWriteOptions>(), false, HConstants.DEFAULT_MAX_FILE_SIZE);

		} finally {
			jsc.stop();
		}

	}

	public static class BulkLoadFunction
			implements Function<String, Pair<KeyFamilyQualifier, byte[]>> {
		@Override
		public Pair<KeyFamilyQualifier, byte[]> call(String v1) throws Exception {
			if (v1 == null) {
				return null;
			}
			
			String[] strs = v1.split(",");
			if (strs.length != 4) {
				return null;
			}

			KeyFamilyQualifier kfq = new KeyFamilyQualifier(Bytes.toBytes(strs[0]),
					Bytes.toBytes(strs[1]), Bytes.toBytes(strs[2]));
			return new Pair(kfq, Bytes.toBytes(strs[3]));

		}

	}

}
