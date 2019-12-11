package com.tongji.utils;

import com.tongji.bean.HBaseConnection;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HBaseOutputFormat implements OutputFormat<String[]> {
    private Connection connection = null;
    private Table table = null;

    public void configure(Configuration parameters) {

    }

    public void open(int i, int i1) throws IOException {
        HBaseConnection instance = HBaseConnection.getInstance();
        connection = instance.getConn();
        TableName tableName = TableName.valueOf("AllUserPortrait");
        table = connection.getTable(tableName);
    }

    public void writeRecord(String[] strings) throws IOException {
        String uid = strings[0];
        String record = strings[1];
        Put put = new Put(Bytes.toBytes(uid), System.currentTimeMillis());
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("jsonData"), Bytes.toBytes(String.valueOf(record)));
        table.put(put);
    }

    public void close() throws IOException {
        table.close();
        connection.close();
    }
}
