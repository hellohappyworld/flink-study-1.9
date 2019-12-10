package com.gaowj.utils;

import java.util.Properties;

public class ConsumerProperties implements KafkaProperties {

    //测试集群
    //    private String BROKER = "10.80.28.154:9092,10.80.29.154:9092,10.80.30.154:9092,10.80.31.154:9092,10.80.32.154:9092";
    //    private String ZOOKEEPER = "10.80.28.154:2181,10.80.29.154:2181,10.80.30.154:2181";

    //数据中心
    //    private String BROKER = "10.90.80.167:9092,10.90.80.168:9092,110.90.81.168:9092,10.90.82.167:9092,10.90.82.168:9092,10.90.83.167:9092,10.90.83.168:9092,10.90.84.167:9092,10.90.84.168:9092";
    //    private String ZOOKEEPER = "10.90.85.168:2181 10.90.86.168:2181 10.90.87.168:2181 10.90.88.168:2181 10.90.89.168:2181";

    //公共集群

    // 用户画像
    private String BROKER = "10.80.3.161:9092,10.80.19.161:9092,10.80.31.161:9092";
    private String ZOOKEEPER = "10.80.10.161:2181 10.80.11.161:2181 10.80.12.161:2181 10.80.14.161:2181 10.80.15.161:2181";

    private String GROUPID;

    public ConsumerProperties(String GROUPID) {
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
