package com.atguigu.handler

import java.{lang, util}
import java.time.LocalDate

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author Skipper
 * @date 2020/08/15
 * @desc
 */
object DauHandler {
  def filterOnSameBatch(filterStartLogDStrean: DStream[StartUpLog]): DStream[StartUpLog] = {
   //转换数据
    val mapDS: DStream[(String, StartUpLog)] = filterStartLogDStrean.map(statLog => {
      (s"${statLog.mid}_${statLog.logDate}", statLog)
    })
    //分组取同组中时间戳最小的一条数据
    val startLogGroupMapSortList: DStream[(String, List[StartUpLog])] = mapDS.groupByKey().mapValues(iter => {
      iter.toList.sortBy(_.ts).take(1)
    })
    //压平
    val resRS: DStream[StartUpLog] = startLogGroupMapSortList.flatMap(_._2)
    resRS
  }

  //对redis中的数据进行跨批次去重
  def filterRedisLog(startLogDStream: DStream[StartUpLog],sc:SparkContext): DStream[StartUpLog] = {

    //方式一: 直接使用filter方法进行过滤
    val resDS1: DStream[StartUpLog] = startLogDStream.filter(startLog => {
      //获取连接
      val client: Jedis = RedisUtil.getJedisClient
      val res: lang.Boolean = client.sismember(s"dau:${startLog.logDate}", startLog.mid)
      //关闭连接
      client.close()
      !res
    })

    //方式二 使用分区操作代替单条数据连接 减少连接数
    val resDS2: DStream[StartUpLog] = startLogDStream.mapPartitions(iter => {
      val client: Jedis = RedisUtil.getJedisClient

      val filterIter: Iterator[StartUpLog] = iter.filter(startLog => {
        !client.sismember(s"dau:${startLog.logDate}", startLog.mid)
      })
      //关闭连接
      client.close()
      filterIter
    })


    //方案三 使用transform加广播变量 每个批次获取一次Redis中的Set集合数据 广播至EXECUTOR
    val resDS3: DStream[StartUpLog] = startLogDStream.transform(rdd => {
      val client: Jedis = RedisUtil.getJedisClient

      val midSet: util.Set[String] = client.smembers("dau:"+LocalDate.now().toString)
      val broadSet: Broadcast[util.Set[String]] = sc.broadcast(midSet)
      client.close()
      //在executor端使用广播变量进行去重
      rdd.filter(startLog => {
        !broadSet.value.contains(startLog.mid)
      })
    })

    resDS3
  }

  def saveMidToRedis(startLogDStream: DStream[StartUpLog]):Unit = {

    startLogDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(iter =>{
        //获取连接
        val client: Jedis = RedisUtil.getJedisClient
        iter.foreach(log =>{
          var key = s"dau:${log.logDate}"

          client.sadd(key,log.mid)
        })
        //关闭连接
        client.close()
      })
    })
  }


}
