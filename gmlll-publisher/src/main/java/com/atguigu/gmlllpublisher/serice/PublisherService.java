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
    public Map getDauTotalHourMap(String date);

    public Double queryOrderTotalAmount(String date);
    public Map<String,Double> queryOrderAmountHourMap(String date);
}
