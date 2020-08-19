package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime}
import java.util
import java.util.Date

import com.aatguigu.gmall.constant.GmallConstants
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.utils.{ElasticUtils, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * @author Skipper
 * @date 2020/08/18 
 * @desc
 */
object AlertApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("alertApp").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    val format = new SimpleDateFormat("yyyy-MM-dd HH")

    val eventLogDStream: DStream[EventLog] = kafkaDStream.map(mapFunc = record => {
      val valueStr: String = record.value()

      val eventLog: EventLog = JSON.parseObject(valueStr, classOf[EventLog])
      val dateArr: Array[String] = format.format(new Date(eventLog.ts)).split(" ")
      eventLog.logDate = dateArr(0)
      eventLog.logHour = dateArr(1)
      eventLog
    })

    //开 5min的窗口

    val window: DStream[EventLog] = eventLogDStream.window(Minutes(5))

    val midToEventLogDStream: DStream[(String, Iterable[EventLog])] = window.map(eventLog => {
      (eventLog.mid, eventLog)
    }).groupByKey()

    val ifCouponCouponAlertInfoDstream: DStream[(Boolean, CouponAlertInfo)] = midToEventLogDStream.map {
      case (mid, logIter) => {
        val uids = new util.HashSet[String]()
        val itemIds = new util.HashSet[String]()
        val events = new util.ArrayList[String]()
        var isClickAction: Boolean = false
        breakable {
          logIter.foreach(eventLog => {

            events.add(eventLog.evid) //用户行为

            if ("coupon".equals(eventLog.evid)) {
              uids.add(eventLog.uid) //用户领券uid
              itemIds.add(eventLog.itemid) //用户领券对应商品id

            } else if ("clickItem".equals(eventLog.evid)) {
              isClickAction = true
              break()
            }
          })
        }

        (uids.size() >= 3 && !isClickAction, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    }

    val filterCouponAlertInfoDstream: DStream[(Boolean, CouponAlertInfo)] = ifCouponCouponAlertInfoDstream.filter(_._1)

    //写入ElasticSearch
    val format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    filterCouponAlertInfoDstream.map(_._2).foreachRDD(rdd => {

      rdd.foreachPartition(iter => {
        val docIdtoData: Iterator[(String, CouponAlertInfo)] = iter.map(alterInfo => {
          val date: String = format1.format(alterInfo.ts)
          (s"${alterInfo.mid}-$date", alterInfo)
        })
        val localdate: String = LocalDate.now().toString
        ElasticUtils.insertByBulk(GmallConstants.GMALL_ES_ALERT_INFO_PRE + "_" + localdate, "_doc",
        docIdtoData.toList)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
