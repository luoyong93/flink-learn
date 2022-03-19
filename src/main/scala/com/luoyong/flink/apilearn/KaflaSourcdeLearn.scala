package com.luoyong.flink.apilearn

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.{KafkaSource, KafkaSourceBuilder}
import org.apache.flink.streaming.api.scala._


object KaflaSourcdeLearn {
  def main(args: Array[String]): Unit = {
    //todo 获取env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //todo 添加source
//    KafkaSource<String> source = KafkaSource.<String>builder()
//      .setBootstrapServers(brokers)
//      .setTopics("input-topic")
//      .setGroupId("my-group")
//      .setStartingOffsets(OffsetsInitializer.earliest())
//      .setValueOnlyDeserializer(new SimpleStringSchema())
//      .build();

//    env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    val kafkaSource: KafkaSource[String] = KafkaSource
        .builder()
        .setBootstrapServers("hadoop103:9092")
        .setTopics("sensor")
        .setGroupId("sensor_learn")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build()
//    env.addSource(FlinkKafkaConsumer)
    val kafkaDS: DataStream[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")

    //todo transform

    //todo sink
    kafkaDS.print()
    //todo exec
    env.execute("kafka_streaming_learn")

  }

}
