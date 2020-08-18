package com.atguigu.app

import java.text.SimpleDateFormat

import com.aatguigu.gmall.constant.GmallConstants
import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
/**
 * @author Skipper
 * @date 2020/08/18 
 * @desc
 */
object OrderApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("orderApp").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(
      GmallConstants.GMALL_TOPIC_ORDER_INFO, ssc)

    val orderInfoDStream: DStream[OrderInfo] = kafkaDS.map(mapFunc = record => {
      val orderInfoStr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])

      orderInfo.consignee_tel = orderInfo.consignee_tel.replace(orderInfo.consignee_tel.substring(3, 7), "****")

      val createArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createArr(0)
      orderInfo.create_hour = createArr(1).split(":")(0)
      orderInfo
    })

    orderInfoDStream.cache()
    orderInfoDStream.print()
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("gmall200317_order_info",
      classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
      new Configuration(),
      Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
