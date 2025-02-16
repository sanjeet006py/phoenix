package org.apache.phoenix.monitoring;

import org.apache.phoenix.log.LogLevel;

import org.junit.Before;
import org.junit.Test;

import static org.apache.phoenix.monitoring.MetricType.TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_QUEUE_WAIT_TIME;
import static org.junit.Assert.assertEquals;

public class ReadRequestMetricsTest {

    private ReadMetricQueue readMetricsQueue;

    @Before
    public void getFreshMetricsObject() {
        readMetricsQueue = new ReadMetricQueue(true, LogLevel.INFO);
    }

    @Test
    public void testTaskMetrics() {
        String tableName = "TABLE1";
        CombinableMetric taskQueueWaitTime = readMetricsQueue.allotMaxMetric(TASK_QUEUE_WAIT_TIME,
                tableName);
        CombinableMetric taskExecutionTime = readMetricsQueue.allotMetric(TASK_EXECUTION_TIME,
                tableName);
        int delta = 10;
        int deltaSum = delta;
        int deltaMax = delta;
        // Test metrics are getting stored fine
        updateAndAssert(taskQueueWaitTime, delta, deltaMax);
        updateAndAssert(taskExecutionTime, delta, deltaSum);

        // Test metrics are getting updated based on CombinableType
        delta = 20;
        deltaMax = delta;
        deltaSum += delta;
        updateAndAssert(taskQueueWaitTime, delta, deltaMax);
        updateAndAssert(taskExecutionTime, delta, deltaSum);

        // Test metrics are getting updated based on CombinableType
        delta = 5;
        deltaSum += delta;
        updateAndAssert(taskQueueWaitTime, delta, deltaMax);
        updateAndAssert(taskExecutionTime, delta, deltaSum);
    }

    public void updateAndAssert(CombinableMetric metric, int delta, int expectedUpdatedValue) {
        metric.change(delta);
        assertEquals(expectedUpdatedValue, metric.getValue());
    }
}
