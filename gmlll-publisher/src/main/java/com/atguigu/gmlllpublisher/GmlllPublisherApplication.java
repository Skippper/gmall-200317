package com.atguigu.gmlllpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmlllpublisher.mapper")
public class GmlllPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmlllPublisherApplication.class, args);
    }

}
