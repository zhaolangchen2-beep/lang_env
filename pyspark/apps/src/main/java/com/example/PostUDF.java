package com.example;

import org.apache.spark.sql.api.java.UDF2;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Q3 后置统计 UDF。
 * 输入: (auction_id: Long, py_duration: Long)
 * 输出: dummy 1L
 *
 * 每 100 万行打印一次吞吐统计。
 */
public class PostUDF implements UDF2<Long, Long, Long> {

    private static final AtomicLong count = new AtomicLong(0);
    private static final AtomicLong totalUdfNs = new AtomicLong(0);
    private static volatile long globalStartTime = System.nanoTime();

    @Override
    public Long call(Long auctionId, Long udfDurationNs) throws Exception {

        long c = count.incrementAndGet();
        if (udfDurationNs != null) {
            totalUdfNs.addAndGet(udfDurationNs);
        }

        if (c % 1_000_000 == 0) {
            synchronized (PostUDF.class) {
                // double-check after acquiring lock
                long currentCount = count.get();
                if (currentCount >= 1_000_000) {
                    long now = System.nanoTime();
                    long wallNs = now - globalStartTime;
                    long udfTotalNs = totalUdfNs.get();
                    double avgOverheadNs = (double) (wallNs - udfTotalNs) / currentCount;
                    double avgUdfNs = (double) udfTotalNs / currentCount;
                    double throughput = currentCount * 1_000_000_000.0 / wallNs;

                    System.out.printf(
                        "[Q3 Benchmark] Count=%d | WallClock=%.1f ms | "
                        + "Avg UDF=%.0f ns | Avg Overhead=%.0f ns | "
                        + "Throughput=%.0f rows/s%n",
                        currentCount,
                        wallNs / 1_000_000.0,
                        avgUdfNs,
                        avgOverheadNs,
                        throughput
                    );

                    // Reset counters
                    count.set(0);
                    totalUdfNs.set(0);
                    globalStartTime = System.nanoTime();
                }
            }
        }
        return 1L;
    }
}
