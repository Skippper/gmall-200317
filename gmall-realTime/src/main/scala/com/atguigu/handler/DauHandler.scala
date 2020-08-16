package com.atguigu.handler

import java.lang
import java.time.LocalDate

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author Skipper
 * @date 2020/08/15 
 * @desc
 */
object DauHandler {
  //对redis中的数据进行跨批次去重
  def filterRedisLog(startLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
    //方法一: 直接使用filter方法进行过滤
    val res1: DStream[StartUpLog] = startLogDStream.filter(startLog => {
      //获取连接
      val client: Jedis = RedisUtil.getJedisClient
      val res: lang.Boolean = client.sismember(s"dau:${startLog.logDate}", startLog.mid)
      //关闭连接
      client.close()
      !res
    })
    res1
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
