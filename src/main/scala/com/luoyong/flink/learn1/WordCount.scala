package com.luoyong.flink.learn1

import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //todo 获取环境
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //todo 添加数据源
//    val socketDS: DataStreamSource[String] = env.socketTextStream("hadoop103", 9999)
    //todo transform 数据转换
//    socketDS.flatMap(_.split(" "))
    //github 修改测试
    // idea 提交测试


    //todo sink

    //todo 执行


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("hadoop103", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }.setParallelism(2)
      .map { (_, 1) }.setParallelism(3)
      .keyBy(_._1)
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)

    counts.print()

    env.execute("Window_Stream_WordCount")
  }

}
