package com.hank.example.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;


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

    public static void addQps(int count) throws IOException {

        //当前时间
        LocalDateTime now = LocalDateTime.now();

        String rowKey = DateTimeFormatter.ofPattern("yyMMdd").format(now);
        String column = DateTimeFormatter.ofPattern("HHmmss").format(now);

        Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
        table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(column), count);
    }

    public void getLongRow(String rowKey) throws IOException {

        LinkedHashMap<String,Long> map = new LinkedHashMap<>();

        Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
        Result result = table.get(new Get(Bytes.toBytes(rowKey)));

        List<Cell> cells = result.listCells();
        for(Cell cell :cells){
            //列名
            map.put(
                    Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()),
                    Bytes.toLong(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength())
            );
        }
    }

}
