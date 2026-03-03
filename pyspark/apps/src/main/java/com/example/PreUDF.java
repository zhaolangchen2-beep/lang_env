package com.example;

import org.apache.spark.sql.api.java.UDF1;

/**
 * 标记时间戳 UDF。
 * 输入 auction_id (Long), 返回当前 nanoTime。
 */
public class PreUDF implements UDF1<Long, Long> {
    @Override
    public Long call(Long value) throws Exception {
        return System.nanoTime();
    }
}