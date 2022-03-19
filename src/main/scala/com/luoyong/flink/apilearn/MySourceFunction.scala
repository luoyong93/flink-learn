package com.luoyong.flink.apilearn

import com.luoyong.flink.entity.Sensor
import org.apache.flink.streaming.api.functions.source.SourceFunction

class MySourceFunction extends SourceFunction[Sensor] {
  var flag:Boolean = true


  override def cancel(): Unit = flag = false

  override def run(ctx: SourceFunction.SourceContext[Sensor]): Unit = {
    while (flag) {
      ctx.collect(Sensor("sensor_1",System.currentTimeMillis(),100.3))
      Thread.sleep(500)
    }
  }
}
