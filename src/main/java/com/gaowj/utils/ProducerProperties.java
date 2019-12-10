package com.gaowj.utils;

import java.util.Properties;

public class ProducerProperties implements KafkaProperties {
    //测试集群
    //    private String BROKER = "10.80.28.154:9092,10.80.29.154:9092,10.80.30.154:9092,10.80.31.154:9092,10.80.32.154:9092";
    //    private String ZOOKEEPER = "10.80.28.154:2181,10.80.29.154:2181,10.80.30.154:2181";

    //数据中心
    private String BROKER = "10.90.80.167:9092,10.90.80.168:9092,110.90.81.168:9092,10.90.82.167:9092,10.90.82.168:9092,10.90.83.167:9092,10.90.83.168:9092,10.90.84.167:9092,10.90.84.168:9092";
    private String ZOOKEEPER = "10.90.85.168:2181 10.90.86.168:2181 10.90.87.168:2181 10.90.88.168:2181 10.90.89.168:2181";

    public Properties getProperties() {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", BROKER);
        prop.setProperty("zookeeper.connect", ZOOKEEPER);

        return prop;
    }
}
