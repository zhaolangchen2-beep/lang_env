package com.example;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class TimedJavaUDF implements UDF2<Double, Long, Row> {
    @Override
    public Row call(Double price, Long javaStartTime) throws Exception {
        long t0 = System.nanoTime();
        double p = price + 1.0;
        long t1 = System.nanoTime();
        
        return RowFactory.create(p, javaStartTime, t1 - t0);
    }
}
