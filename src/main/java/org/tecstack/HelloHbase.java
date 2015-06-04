package org.tecstack;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * Don't forget to add entries in /etc/hosts for hostnames in hbase-site.xml when testing.
 */
public class HelloHbase {

	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();

		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = new HTableDescriptor("hellohdfs_test");
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);
		byte[] tablename = htd.getName();
		HTableDescriptor[] tables = admin.listTables();
		for (HTableDescriptor hTableDescriptor : tables) {
			System.out.println(hTableDescriptor);
		}

		HTable table = new HTable(config, tablename);
		byte[] row1 = Bytes.toBytes("row1");
		Put p1 = new Put(row1);
		byte[] databytes = Bytes.toBytes("data");
		p1.add(databytes, Bytes.toBytes("1"), Bytes.toBytes("value1"));
		table.put(p1);

		Get g = new Get(row1);
		Result result = table.get(g);
		System.out.println("Get: " + result);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);

		try {
			for (Result scannerResult : scanner) {
				System.out.println("Scan: " + scannerResult);
			}
		} finally {
			scanner.close();
		}
		
		admin.disableTable(tablename);
		admin.deleteTable(tablename);
	}

}
