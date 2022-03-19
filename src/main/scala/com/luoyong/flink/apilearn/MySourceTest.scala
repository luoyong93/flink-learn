package com.luoyong.flink.apilearn

import com.luoyong.flink.entity.Sensor
import org.apache.flink.streaming.api.scala._

object MySourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val myDS: DataStream[Sensor] = env.addSource(new MySourceFunction())
    myDS.print()

    env.execute("mysource_test")
  }

}
