package com.example;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;

public class JobTimeListener extends SparkListener {
    private long jobStartTime = -1;
    private long jobEndTime = -1;

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        jobStartTime = System.nanoTime();
        System.out.printf("[Job Start] Job ID=%d, Time(ns)=%d%n", 
                         jobStart.jobId(), jobStartTime);
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        jobEndTime = System.nanoTime();
        long durationNs = jobEndTime - jobStartTime;
        double durationSec = durationNs / 1e9;
        
        System.out.printf("[Job End] Job ID=%d, Time(ns)=%d%n", 
                         jobEnd.jobId(), jobEndTime);
        System.out.printf("[Job Summary] Job ID=%d, Duration=%.2f sec%n", 
                         jobEnd.jobId(), durationSec);
    }
}
