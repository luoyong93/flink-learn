package com.luoyong.flink.tranformlearn

import com.luoyong.flink.entity.Sensor
import org.apache.flink.streaming.api.scala._

/**
 * flink 算子练习
 * flink算子一般分为三类：
 *  简单算子 map flatmap fliter ...
 *  key类型 ：keyby 后面对应聚合 sum min max minby maxby
 *  多流算子：
 *  split  已经被弃用 采用process
 *  connect
 *  union
 *
 *
 */
object SimpleOperator {
  def main(args: Array[String]): Unit = {
    //todo 获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //todo source
    val textDS: DataStream[String] = env.readTextFile("E:\\flink\\src\\main\\resources\\data.txt")
//    val textDS: DataStream[String] = env.readTextFile("data.txt")
//    textDS.flatMap(_.split(",")).print()
    val sensorDS: DataStream[Sensor] = textDS.map(data => {
      val dataArr: Array[String] = data.split(",")
      Sensor(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    //todo sink
//    sensorDS.keyBy(_.id)
//      .maxBy("temperature").print()
    sensorDS.keyBy(_.id)
      .reduce((sensor1,sensor2)=>{

        Sensor(sensor2.id,sensor2.timestamp,math.max(sensor1.temperature,sensor2.temperature))
      }).print()
    //todo exec
    env.execute()



  }


}
