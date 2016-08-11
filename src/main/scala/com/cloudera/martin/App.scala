package com.cloudera.martin


import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}


/**
 * @author ${user.name}
 */
object App {
  
//  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setMaster("yarn-cluster").setAppName("streaming-test")
    val sc = new SparkContext(conf)

    var batchInterval = 10
    val kafkaBrokers = "172.31.21.38:9092,172.31.30.18:9092,172.31.22.136:9092"
    val kafkaInputTopic = "optus_test"
    val sparkOutputTopic = "optus_output"

    val ssc = new StreamingContext(sc, Seconds(batchInterval))     //batch internal
    val kafkaParams = Map("metadata.broker.list" -> kafkaBrokers)
    val topic = Set(kafkaInputTopic)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)

    //write DStream output to another topic in kafka
    wordCounts.foreachRDD( rdd => {
//      System.out.println("# events = " + rdd.count())
//
//      //Save RDD to verify the rdd result   //Works good here
      rdd.saveAsTextFile("hdfs://nameservice1/tmp/optus_test/file")

      rdd.foreachPartition( partition => {
        val kafkaOpTopic = sparkOutputTopic
        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)
        partition.foreach( record => {
          val data = record.toString
          val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
          producer.send(message)
        } )
        producer.close()
      })
    })

    wordCounts.print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(batchInterval * 5 * 1000)
  }

}
