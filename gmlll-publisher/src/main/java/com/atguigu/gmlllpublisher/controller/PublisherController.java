package com.atguigu.gmlllpublisher.controller;


import com.atguigu.gmlllpublisher.serice.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Skipper
 * @date 2020/08/17
 * @desc 获取数据接口
 */
@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @GetMapping("/realtime-total")
    public String getDauTotal(String date){

        Integer totalCount = publisherService.getDauTotalCount(date);   //日活总数
        //结果数据集合
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>(){{
            add(new HashMap<>());
            add(new HashMap<>());
        }};
        //数据1
        Map<String, Object> map1 = list.get(0);
            map1.put("id","dau");
            map1.put("name","新增日活");
            map1.put("value",totalCount);
        //数据2
        Map<String, Object> map2 = list.get(1);
            map2.put("id","dau");
            map2.put("name","新增用户");
            map2.put("value",233);
        return list.toString();
    }
}
