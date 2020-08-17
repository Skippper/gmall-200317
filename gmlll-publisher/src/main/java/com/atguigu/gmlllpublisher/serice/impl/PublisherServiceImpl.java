package com.atguigu.gmlllpublisher.serice.impl;

import com.atguigu.gmlllpublisher.mapper.DauMapper;
import com.atguigu.gmlllpublisher.serice.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
    public List<Map> getDauTotalHourMap(String date) {
        return mapper.getDauTotalHourMap(date);
    }
}
