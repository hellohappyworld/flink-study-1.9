package com.gaowj.TableSQLApi.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * Created on 2019-11-12.
  * original -> https://github.com/GatsbyNewton/learning-flink/blob/870820a474/flink-table/src/main/scala/edu/wzm/source/MySourceFunction.scala
  */

class MySourceFunction[T](dataWithTimestamps: Seq[Either[(Long, T), Long]]) extends SourceFunction[T] {
  override def cancel(): Unit = ???

  override def run(sourceContext: SourceFunction.SourceContext[T]): Unit = {
    dataWithTimestamps.foreach {
      case Left(l) => sourceContext.collectWithTimestamp(l._2, l._1)
      case Right(r) => sourceContext.emitWatermark(new Watermark(r))
    }
  }
}
