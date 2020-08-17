package com.atguigu.gmlllpublisher.serice.impl;

import com.atguigu.gmlllpublisher.mapper.DauMapper;
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
    private DauMapper mapper;
    @Override
    public Integer getDauTotalCount(String date) {
        return mapper.getDauTotalCount(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {
        List<Map> mapList = mapper.getDauTotalHourMap(date);
        Map<String,Long> returnMap = new HashMap<>();

        for (Map map : mapList) {
            returnMap.put( map.get("LH").toString(), (Long) map.get("CT"));
        }
        return returnMap;
    }
}
