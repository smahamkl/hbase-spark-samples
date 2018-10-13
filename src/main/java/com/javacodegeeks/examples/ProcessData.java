package com.javacodegeeks.examples;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.function.BiFunction;
import java.util.function.Function;
/*import org.apache.log4j.Logger;
import org.apache.log4j.Level;*/
import mjson.Json;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


/**
 * An example Spark processing of HBase data, from Github
 * GIST:https://gist.github.com/bolerio/f70c374c3e96495f5a69
 * 
 * @author https://github.com/bolerio
 */
public class ProcessData {
	private Configuration conf; // HBase configuration object
	private SparkConf sparkconf; // Spark configuration object
	private JavaSparkContext jsc; // Spark context, the Java version of it
	private JavaHBaseContext hbaseContext; // A context object for the HBase-Spark integration module.
	private SQLContext sqlContext; // A Spark SQL processing context

	/**
	 * Obtain the runtime Java value of the latest version of the HBase cell
	 * identified by the coordinates 'family' and 'qualifier' from the given result
	 * row.
	 * 
	 * @param result    An HBase table result row as obtain from a scan operation.
	 * @param family    The HBase column family.
	 * @param qualifier The column qualifier.
	 * @param converter A function that converts a raw byte[] coming from HBase to
	 *                  the desired Java runtime type.
	 * @return The latest value of the identified cell.
	 */
	static final <T> Json value(Result result, String family, String qualifier,
			BiFunction<byte[], Integer, T> converter) {
		Cell cell = result.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		if (cell == null)
			return Json.nil();
		else
			return Json.make(converter.apply(cell.getValueArray(), cell.getValueOffset()));
	}

	/**
	 * A function object that converts an HBase result row into a runtime Json
	 * object.
	 */
	static final Function<Result, Json> tojsonMap = (result) -> {
		Json data = Json.object().set("name", value(result, "arrest", "name", Bytes::toString))
				.set("bookdate", value(result, "arrest", "bookdate", Bytes::toString))
				.set("dob", value(result, "arrest", "dob", Bytes::toString))
				.set("charge1", value(result, "charge", "charge1", Bytes::toString))
				.set("charge2", value(result, "charge", "charge2", Bytes::toString))
				.set("charge3", value(result, "charge", "charge3", Bytes::toString));
		return data;
	};

	private void init() {
		// Default HBase configuration for connecting to localhost on default port.
		conf = HBaseConfiguration.create();

		String path = ImportData.class.getClassLoader().getResource("hbase-site.xml").getPath();
		conf.addResource(new Path(path));

		// Simple Spark configuration where everything runs in process using 2 worker
		// threads.
		sparkconf = new SparkConf().setAppName("Jail With Spark").setMaster("local[2]");
		// The Java Spark context provides Java-friendly interface for working with
		// Spark RDDs
		jsc = new JavaSparkContext(sparkconf);
		// The HBase context for Spark offers general purpose method for bulk read/write
		hbaseContext = new JavaHBaseContext(jsc, conf);
		// The entry point interface for the Spark SQL processing module. SQL-like data
		// frames
		// can be created from it.
		sqlContext = new org.apache.spark.sql.SQLContext(jsc);
	}

	public ProcessData() {
		init();
	}

	/**
	 * Return a SQL-like DataFrame for the jail data table. An HBase row is
	 * essentially mapped to a SQL row where each column has a specific data type.
	 * In our case all columns are of type string. The DataFrame can be seen as a
	 * runtime view of the underlying data - as everything Spark, it is lazily
	 * computed.
	 */
	public DataFrame jailDataFrame() {
		String sqlMapping = "name STRING arrest:name" + "," + "bookdate STRING arrest:bookdate" + ","
				+ "dob STRING arrest:dob" + "," + "location STRING arrest:location" + ","
				+ "charge1 STRING charge:charge1" + "," + "charge2 STRING charge:charge2" + ","
				+ "charge3 STRING charge:charge3";
		DataFrame dfJail = sqlContext.read().format("org.apache.hadoop.hbase.spark")
				.option("hbase.columns.mapping", sqlMapping).option("hbase.table", "jaildata").load();
		// This is useful when issuing SQL text queries directly against the sqlContext
		// object.
		return dfJail;
	}

	/**
	 * Return an RDD representing the arrests data set as a stream of Json records.
	 */
	public JavaRDD<Json> jailData() {
		try {
			// Here we just tell the hbaseContext (from the HBase-Spark module) to
			// scan our jaildata table using default scanning options. Look at the Scan
			// class to see what options are available for the scanning process. For
			// example, it is possible to define filters that will be executed at the
			// database level and some other things.
			return hbaseContext.hbaseRDD(TableName.valueOf("jaildata"), new Scan())
					.map(tuple -> tojsonMap.apply(tuple._2()));
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static void main(String[] argv) {
		// Spark is a bit chatty on the console. Since we perform some simple
		// computation and display
		// a few numbers, we want to shut off the logging here. Comment those two lines
		// in case
		// something goes wrong and you want to see more log output.
		/*
		 * Logger.getLogger("org").setLevel(Level.OFF);
		 * Logger.getLogger("akka").setLevel(Level.OFF);
		 */

		// This object will now initialize connection with HBase and make the Spark
		// context
		// ready for action.
		ProcessData process = new ProcessData();

		// We will be filtering offenders that are 30 years old or less from
		// those that are older. So we need the ability to parse dates and a value
		// to compare against.
		final SimpleDateFormat dobFormat = new SimpleDateFormat("MM/dd/yyyy");
		Calendar thirtyYearsAgo = Calendar.getInstance();
		thirtyYearsAgo.add(Calendar.YEAR, -30);

		// This function is a filter for arrests that contain the 'BATTERY' charge in
		// either of the charge1, charge2 or charge3 fields. Since our data has up
		// to 3 possible charges per arrest, we need to look at each one of them.
		org.apache.spark.api.java.function.Function<Json, Boolean> chargeFilter = j -> j.at("charge1").toString()
				.contains("BATTERY") || j.at("charge2").toString().contains("BATTERY")
				|| j.at("charge3").toString().contains("BATTERY");

		// The following statement obtains all jail data from the database and filters
		// for
		// arrests of people older than 30 (their DOB - date of birth - is before thirty
		// years ago)
		// that have been charged with some for of BATTERY. Then it simply counts all
		// filtered
		// records. Note that only when the 'count' method is called does any actual
		// data
		// scanning and filtered occur.
		long x = process.jailData()
				.filter(j -> dobFormat.parse(j.at("dob").asString()).before(thirtyYearsAgo.getTime()))
				.filter(chargeFilter).count();
		// Same as above, but for people younger than thirty
		long y = process.jailData().filter(j -> dobFormat.parse(j.at("dob").asString()).after(thirtyYearsAgo.getTime()))
				.filter(chargeFilter).count();
		System.out.println("Older 'battery' offenders: " + x + ", younger 'battery' offenders: " + y
				+ ", ratio(older/younger)=" + ((double) x) / y);

		// Now let's show off the Spark SQL capabilities.

		// First, SQL-like queries can be made via a fluid API of a DataFrame. You can
		// think of a DataFrame
		// as SQL relation. Not a "table", but a "relation", or a dynamic "view" if you
		// will. You can perform
		// various transformations, filterings and joins on DataFrames to yield other
		// DataFrames and just as with
		// RDDs, computation occurs only on demand. In fact, there is a sophisticated
		// query planner that will
		// first pre-process the operations you've request and construct a query plan
		// before executing it.
		//
		// In this example, we simply look for the keyword "THEFT" anyway in the arrest
		// charges to count
		// the number of theft-related arrests.
		DataFrame dfJail = process.jailDataFrame();
		long theftCount = dfJail
				.select(dfJail.col("charge1").contains("THEFT")
						.or(dfJail.col("charge2").contains("THEFT").or(dfJail.col("charge3").contains("THEFT"))))
				.count();
		System.out.println("Thefts: " + theftCount);
		
		dfJail.registerTempTable("jaildata");
		
		//update: 2018-10-13  where clause not working inside sql
		System.out.println(process.sqlContext.sql("select count(*) as cnt from jaildata").first().get(0));
//				.sql("select count(*) as cnt from jaildata where (charge1='CONTEMPT OF COURT' or "
	//					+ "charge2='CONTEMPT OF COURT' or charge3='CONTEMPT OF COURT')").first().get(0));

	}
}