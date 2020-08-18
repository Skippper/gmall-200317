package com.atguigu.gmlllpublisher.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @author Skipper
 * @date 2020/08/18
 * @desc
 */
//@Mapper
public interface OrderMapper {

//    @Select("select sum(total_amount) sum_amount from gmall200317_order_info where create_date=#{date}")
    public Double queryOrderTotalAmount(String date);
//    @Select("select create_hour, cast(sum(total_amount) as double) sum_amount from gmall200317_order_info where create_date=#{date}  group by create_hour")
    public List<Map<String,Object>> queryOrderAmountHourMap(String date);
}
