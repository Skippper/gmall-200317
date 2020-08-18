package com.atguigu.gmlllpublisher.serice.impl;

import com.atguigu.gmlllpublisher.mapper.DauMapper;
import com.atguigu.gmlllpublisher.mapper.OrderMapper;
import com.atguigu.gmlllpublisher.serice.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Skipper
 * @date 2020/08/17
 * @desc
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;
    @Override
    public Integer getDauTotalCount(String date) {
        return dauMapper.getDauTotalCount(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {
        List<Map> mapList = dauMapper.getDauTotalHourMap(date);
        Map<String,Long> returnMap = new HashMap<>();

        for (Map map : mapList) {
            returnMap.put( map.get("LH").toString(), (Long) map.get("CT"));
        }
        return returnMap;
    }

    @Override
    public Double queryOrderTotalAmount(String date) {
        return orderMapper.queryOrderTotalAmount(date);
    }

    @Override
    public Map<String, Double> queryOrderAmountHourMap(String date) {

        Map<String,Double> resMap = new HashMap<>();

        List<Map<String, Object>> queryMapList = orderMapper.queryOrderAmountHourMap(date);

        for (Map<String, Object> map : queryMapList) {
            resMap.put(map.get("CREATE_HOUR").toString(),(Double) map.get("SUM_AMOUNT"));
        }
        return resMap;
    }

}
