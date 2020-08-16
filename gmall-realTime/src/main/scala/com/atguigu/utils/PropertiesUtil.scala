package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author Skipper
 * @date 2020/08/15 
 * @desc
 */
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}

