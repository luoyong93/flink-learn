package com.luoyong.flink.tranformlearn

import com.luoyong.flink.entity.Sensor
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 多流算子联系
 */
object MultipleOperator {
  def main(args: Array[String]): Unit = {
    //todo env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //todo source
    val textDS: DataStream[String] = env.readTextFile("E:\\flink\\src\\main\\resources\\data.txt")
    //todo tarnsform
    val sensorDS: DataStream[Sensor] = textDS.filter(_.length > 0).map(
      data => {
        val dataArr: Array[String] = data.split(",")
        Sensor(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
      }
    )

    //分流处理 37°以上为高温 低于37°为低温
    val high = new OutputTag[Sensor]("high")
    val low = new OutputTag[Sensor]("low")

    val sideoutputDS: DataStream[Sensor] = sensorDS.process(new ProcessFunction[Sensor, Sensor] {
      override def processElement(value: Sensor, ctx: ProcessFunction[Sensor, Sensor]#Context, out: Collector[Sensor]) = {
        value.temperature match {
          case a if a > 37 => ctx.output(high, value)
          case _ => ctx.output(low, value)
        }
      }
    })
    //todo sink
    val highDS: DataStream[Sensor] = sideoutputDS.getSideOutput(high)
    val lowDS: DataStream[Sensor] = sideoutputDS.getSideOutput(low)
    val connectDS: DataStream[Product] = highDS.connect(lowDS).map(sensor1 => {
      (sensor1.id, sensor1.temperature)
    }, sensor2 => {
      (sensor2.id, sensor2.timestamp, sensor2.temperature)
    })
//    connectDS.
    connectDS.print()
    //todo exec
    env.execute("multiple_test")
  }

}
