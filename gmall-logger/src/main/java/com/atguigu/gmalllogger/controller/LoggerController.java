package com.atguigu.gmalllogger.controller;

import com.aatguigu.gmall.constant.GmallConstants;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;


@Slf4j
@RestController //Controller+responsebody
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;


    @GetMapping("/test")
    public String test( String sd){
        System.out.println("name : " + sd);
        return sd;
    }

    @RequestMapping("/log")
    public void sendLog(String logString){

        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());

        if ("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        }else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }
        log.info(jsonObject.toJSONString());
    }
}

