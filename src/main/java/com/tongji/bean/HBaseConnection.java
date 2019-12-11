package com.tongji.bean;

import com.tongji.common.PropertiesConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnection {
    private static class HBaseConnHolder {
        private static final HBaseConnection INSTANCE = new HBaseConnection();
    }

    private HBaseConnection() {

    }

    public static final HBaseConnection getInstance() {
        return HBaseConnHolder.INSTANCE;
    }

    public Connection getConn() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, PropertiesConstants.HBASE_ZOOKEEPER_QUORUM);
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        configuration.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "120000");
        configuration.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "120000");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }
}
