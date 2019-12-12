package com.tongji.utils;

import java.util.Properties;

public class ConsumerProperties implements KafkaProperties {
    private String BROKER = null;
    private String ZOOKEEPER = null;
    private String GROUPID = null;

    public ConsumerProperties(String BROKER, String ZOOKEEPER, String GROUPID) {
        this.BROKER = BROKER;
        this.ZOOKEEPER = ZOOKEEPER;
        this.GROUPID = GROUPID;
    }

    public Properties getProperties() {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", BROKER);
        prop.setProperty("zookeeper.connect", ZOOKEEPER);
        prop.setProperty("group.id", GROUPID);

        return prop;
    }
}
