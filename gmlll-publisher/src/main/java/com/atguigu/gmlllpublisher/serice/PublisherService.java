package com.atguigu.gmlllpublisher.serice;

import java.util.List;
import java.util.Map;

/**
 * @author Skipper
 * @date 2020/08/17
 * @desc
 */
public interface PublisherService {

    public Integer getDauTotalCount(String date);
    public List<Map> getDauTotalHourMap(String date);
}
