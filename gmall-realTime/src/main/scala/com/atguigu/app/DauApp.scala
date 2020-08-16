package com.atguigu.app

import java.text.SimpleDateFormat

import com.aatguigu.gmall.constant.GmallConstants
import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Skipper
 * @date 2020/08/15 
 * @desc
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("dauApp").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(conf,Seconds(5))
    //3.读取Kafka Start主题的数据创建流
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.将读取的数据转换为样例类对象(logDate和logHour)
    val format = new SimpleDateFormat("yyyy-MM-dd HH")
    val startLogDStream: DStream[StartUpLog] = kafkaDS.map(record => {
      val jsonStr: String = record.value()
      //转换成Json字符串 封装成样例类
      val startUpLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])

      val date: String = format.format(startUpLog.ts)
      val dateArr: Array[String] = date.split(" ")

      startUpLog.logDate = dateArr(0)
      startUpLog.logHour = dateArr(1)
      startUpLog
    })

    startLogDStream.cache()
    println("去重前有" +  startLogDStream.count().print() + "条")
    //5.根据Redis中保存的数据进行跨批次去重
    val  filterStartLogDStrean : DStream[StartUpLog] = DauHandler.filterRedisLog(startLogDStream)
    filterStartLogDStrean.cache()
    println("去重后有" +  filterStartLogDStrean.count().print() + "条")
    //6.对第一次去重后的数据做同批次去重

    //7.将两次去重后的数据(mid)写入Redis
    DauHandler.saveMidToRedis(filterStartLogDStrean)
    //8.将数据保存至HBase(Phoenix)

    //9.启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
