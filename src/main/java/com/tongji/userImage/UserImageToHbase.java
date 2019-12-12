package com.tongji.userImage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tongji.common.PropertiesConstants;
import com.tongji.utils.ConsumerProperties;
import com.tongji.utils.HBaseOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Iterator;
import java.util.Properties;

public class UserImageToHbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);

        ConsumerProperties consumerProperties = new ConsumerProperties(PropertiesConstants.USERIMAGE_BROKER, PropertiesConstants.USERIMAGE_ZOOKEEPER, "userImage_test01");
        Properties properties = consumerProperties.getProperties();
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("cimg", new SimpleStringSchema(), properties);
        myConsumer.setStartFromLatest();

        DataStreamSource<String> source = env.addSource(myConsumer);

        SingleOutputStreamOperator<String> uid = source.filter(new FilterFunction<String>() {
            public boolean filter(String s) throws Exception {
                return s.contains("uid") && !StringUtils.isBlank(s);
            }
        });

        SingleOutputStreamOperator<String[]> parse = uid.map(new MapFunction<String, String[]>() {
            public String[] map(String s) throws Exception {
                JSONObject jstr = JSON.parseObject(s);
                String uid = jstr.getString("uid");
                String jsonField = "docpic_cate,video_cate,recent_docpic_cate,recent_video_cate,docpic_cotag,video_cotag,recent_docpic_cotag,recent_video_cotag,suppose_gender,suppose_job";
                JSONObject nObject = new JSONObject();
                try {
                    for (String field : jsonField.split(",")) {
                        if (jstr.containsKey(field))
                            nObject.put(field, jstr.getString(field));
                    }
                    Iterator<String> itor = jstr.keySet().iterator();
                    while (itor.hasNext()) {
                        String key = itor.next();
                        if (!nObject.containsKey(key)) {
                            nObject.put(key, "");
                        }
                    }
                } catch (Exception e) {
                    System.out.println(jstr.toJSONString());
                }
                String jsonStr = nObject.toJSONString();
                String[] out = {uid, jsonStr};
                return out;
            }
        });

        SingleOutputStreamOperator<String[]> filter = parse.filter(new FilterFunction<String[]>() {
            public boolean filter(String[] strings) throws Exception {
                return !org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(strings[0]);
            }
        });

        filter.writeUsingOutputFormat(new HBaseOutputFormat()).name("toHbase");

        env.execute("UserImageToHbase");
    }
}
