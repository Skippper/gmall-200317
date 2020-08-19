package com.atguigu.bean

/**
 * @author Skipper
 * @date 2020/08/18 
 * @desc
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)

