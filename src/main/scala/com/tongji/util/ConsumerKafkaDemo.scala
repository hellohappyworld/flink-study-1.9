package com.tongji.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object ConsumerKafkaDemo {
  private val ZOOKEEPER_HOST: String = "10.80.28.154:2181,10.80.29.154:2181,10.80.30.154:2181"
  private val KAFKA_BROKER: String = "10.80.28.154:9092,10.80.29.154:9092,10.80.30.154:9092,10.80.31.154:9092,10.80.32.154:9092"

  private val TRANSACTION_GROUP: String = "sdjkfjksdsdiwc"

  private val TOPIC: String = "appsta"


  /**
    * 消费KAFKA数据
    *
    * @param env
    */
  def getKafkaData(env: StreamExecutionEnvironment) = {
    val prop: Properties = new Properties()
    prop.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    prop.setProperty("bootstrap.servers", KAFKA_BROKER)
    prop.setProperty("group.id", TRANSACTION_GROUP)
    //    prop.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "600000")
    //    prop.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000")
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](TOPIC, new SimpleStringSchema(), prop)
    //    kafkaConsumer.setStartFromLatest()
    kafkaConsumer.setStartFromEarliest()
    val kafkaDStream: DataStream[String] = env.addSource(kafkaConsumer)

    kafkaDStream
  }

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment


    getKafkaData(env)
      .print()

    env.execute("ConsumerKafkaDemo")
  }
}
