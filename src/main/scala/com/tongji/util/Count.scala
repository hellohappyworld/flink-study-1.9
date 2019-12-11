package com.tongji.util

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * 统计条数
  */
object Count {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val shengchan = env.readTextFile("C:\\Users\\gaowj\\Desktop\\yanshu\\appsta_old_kafka")
      .filter(!_.equals(""))
      .map(line => {
        val arr = line.split("_")
        (arr(0).toLong, arr(1).toLong)
      })
      .filter(tup => tup._1 >= 201911091400L && tup._1 < 201911121400L)
      .map(_._2)
      .reduce(_ + _)
    val shengchanArr = shengchan.collect.toArray
    println("生产集群" + shengchanArr(0))

    val xinjiqun = env.readTextFile("C:\\Users\\gaowj\\Desktop\\yanshu\\appsta_xin_kafka")
      .filter(!_.equals(""))
      .map(line => {
        val arr = line.split("_")
        (arr(0).toLong, arr(1).toLong)
      })
      .filter(tup => tup._1 >= 201911091400L && tup._1 < 201911121400L)
      .map(_._2)
      .reduce(_ + _)
    val xinjiqunArr: Array[Long] = xinjiqun.collect.toArray
    println("新集群" + xinjiqunArr(0))

    val cha: Long = shengchanArr(0) - xinjiqunArr(0)

    println("相差" + cha)

  }
}
