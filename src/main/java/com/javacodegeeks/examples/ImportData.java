package com.javacodegeeks.examples;

import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.coprocessor.Export;

/**
 * This is a very simple main program showing the bare minimum to get data from
 * a CSV file into a newly created HBase table. The CSV file itself can be found
 * at the Github GIST: https://gist.github.com/bolerio/f70c374c3e96495f5a69
 * 
 * @author https://github.com/bolerio
 */

public class ImportData {

	public static void main(String[] argv) throws Exception {
		Configuration config = HBaseConfiguration.create();

		String path = ImportData.class.getClassLoader().getResource("hbase-site.xml").getPath();
		config.addResource(new Path(path));

/*		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum", "127.0.1.1");
		config.set("zookeeper.znode.parent", "/hbase-unsecure");*/

		int rowCount = 100;

		byte[] family = Bytes.toBytes("family");

		Configuration conf = HBaseConfiguration.create();

		TableName tableName = TableName.valueOf("ExportEndpointExample");

		try (Connection connection = ConnectionFactory.createConnection(conf);

				Admin admin = connection.getAdmin()) {
			TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
					// MUST mount the export endpoint
					.setCoprocessor(Export.class.getName())
					.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family))
					.build();

			admin.createTable(desc);

			List<Put> puts = new ArrayList<>(rowCount);

			for (int row = 0; row != rowCount; ++row) {
				byte[] bs = Bytes.toBytes(row);
				Put put = new Put(bs);
				put.addColumn(family, bs, bs);
				puts.add(put);
			}

			try (Table table = connection.getTable(tableName)) {
				table.put(puts);
			}

			// We are creating an HBase connection and an Admin object that will allows use
			// to create
			// a new HBase table
			// We assumed that the jaildata.csv file is in /tmp, adjust your location
			// accordingly.
			CSVParser parser = CSVParser.parse(Paths.get("jaildata.csv").toFile(), Charset.defaultCharset(),
					CSVFormat.EXCEL);
			Iterator<CSVRecord> recordIterator = parser.iterator();
			recordIterator.next(); // skip titles

			// The table descriptor is essentially a schema definition object. The HBase
			// column families
			// associated with a table need to be specified in advance. Particular column
			// qualifiers are
			// open-ended and each column family can have any number of column qualifiers
			// added ad hoc.
			//
			// We have to column families: arrest and charge. The idea is that the charge
			// column family
			// will contain possible charges of which there are few varieties while the
			// arrest column family
			// will contain values describing the individual and the occurrence of the
			// arrest.
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("jaildata"));
			tableDescriptor.addFamily(new HColumnDescriptor("arrest"));
			tableDescriptor.addFamily(new HColumnDescriptor("charge"));
			admin.createTable(tableDescriptor);
			Table table = connection.getTable(TableName.valueOf("jaildata"));

			// Now we are ready to read the CSV and store in the HBase table
			while (recordIterator.hasNext()) {
				CSVRecord csvRecord = recordIterator.next();
				ArrayList<Put> puts1 = new ArrayList<Put>();
				if (csvRecord.size() > 0 && csvRecord.get(0).trim().length() > 0) {
					// The CSV record's format is like this:
					// BookDate,Defendant,DOB,ChargeCode1,Charge1,ChargeCode2,Charge2,ChargeCode3,Charge3,Location
					String rowKey = csvRecord.get(1) + " -- " + csvRecord.get(0);
					Put put = new Put(Bytes.toBytes(rowKey));
					put.addColumn(Bytes.toBytes("arrest"), Bytes.toBytes("name"), Bytes.toBytes(csvRecord.get(1)));
					put.addColumn(Bytes.toBytes("arrest"), Bytes.toBytes("bookdate"), Bytes.toBytes(csvRecord.get(0)));
					put.addColumn(Bytes.toBytes("arrest"), Bytes.toBytes("dob"), Bytes.toBytes(csvRecord.get(2)));
					put.addColumn(Bytes.toBytes("arrest"), Bytes.toBytes("location"), Bytes.toBytes(csvRecord.get(9)));
					put.addColumn(Bytes.toBytes("charge"), Bytes.toBytes("charge1"), Bytes.toBytes(csvRecord.get(4)));
					put.addColumn(Bytes.toBytes("charge"), Bytes.toBytes("charge2"), Bytes.toBytes(csvRecord.get(6)));
					put.addColumn(Bytes.toBytes("charge"), Bytes.toBytes("charge3"), Bytes.toBytes(csvRecord.get(8)));
					puts1.add(put);
				}
				table.put(puts1);
			}
			Path output = new Path("/tmp/ExportEndpointExample_output");

			Scan scan = new Scan();

			Map<byte[], Export.Response> result = Export.run(conf, tableName, scan, output);

			final long totalOutputRows = result.values().stream().mapToLong(v -> v.getRowCount()).sum();

			final long totalOutputCells = result.values().stream().mapToLong(v -> v.getCellCount()).sum();

			System.out.println("table:" + tableName);

			System.out.println("output:" + output);

			System.out.println("total rows:" + totalOutputRows);

			System.out.println("total cells:" + totalOutputCells);
		} catch (Throwable t) {
			t.printStackTrace(System.err);
		}
	}
}