package com.atguigu.gmlllpublisher.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @author Skipper
 * @date 2020/08/17
 * @desc
 */
//@Mapper
public interface DauMapper {
//    @Select(" select count(*) from GMALL200317_DAU where  logdate=#{date}")
    public Integer getDauTotalCount(String date);
    /*@Select("  select LOGHOUR lh, count(*) ct from GMALL200317_DAU where  LOGDATE=#{date}\n" +
            "        group by LOGHOUR")*/
    public List<Map> getDauTotalHourMap(String date);
}
