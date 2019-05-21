package com.hank.example.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HbaseUtil {

    private static Connection conn;

    public static String ZK_HOST = "192.168.1.191";
    public static String ZK_PORT ="2181";

    public static String TABLE_NAME = "xmiss_qps";
    public static String COLUMN_FAMILY_NAME = "second";

    static{

        try {
            //连接数据库
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum",ZK_HOST);
            conf.set("hbase.zookeeper.property.clientPort",ZK_PORT);
            conn = ConnectionFactory.createConnection(conf);

            create();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Create Table
    public static void create() throws IOException{
        Admin admin = conn.getAdmin();

        TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY_NAME)).build())
                .build();

        admin.createTable(table);
    }
}
