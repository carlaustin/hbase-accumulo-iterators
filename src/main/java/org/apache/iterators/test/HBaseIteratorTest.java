package org.apache.iterators.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;


public class HBaseIteratorTest {

    public static final int NO_TO_WRITE = 10;

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "192.168.15.200");
//        conf.set("hbase.zookeeper.property.clientPort","2181");
//        conf.set("hbase.master", "192.168.15.200:60000");
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists("test")) {
            admin.disableTable("test");
            admin.deleteTable("test");
        }
        HTableDescriptor table = new HTableDescriptor("test");
        table.addFamily(new HColumnDescriptor("data"));
        admin.createTable(table);
        HTable testTable = new HTable(conf, "test");
        for (int i = 0; i < NO_TO_WRITE; i++) {
            Put put = new Put(new String("row" + i).getBytes());
            put.add("data".getBytes(), new String("row" + i).getBytes(), new String("value" + i).getBytes());
            testTable.put(put);
        }
        testTable.close();
    }

}
