package com.tongji.utils;

import java.util.Properties;

public class ProducerProperties implements KafkaProperties {
    private String BROKER = null;
    private String ZOOKEEPER = null;

    public ProducerProperties(String BROKER, String ZOOKEEPER) {
        this.BROKER = BROKER;
        this.ZOOKEEPER = ZOOKEEPER;
    }

    public Properties getProperties() {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", BROKER);
        prop.setProperty("zookeeper.connect", ZOOKEEPER);

        return prop;
    }
}
