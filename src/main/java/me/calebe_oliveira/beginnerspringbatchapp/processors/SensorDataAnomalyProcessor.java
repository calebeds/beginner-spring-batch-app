package me.calebe_oliveira.beginnerspringbatchapp.processors;

import me.calebe_oliveira.beginnerspringbatchapp.domain.AnomalyType;
import me.calebe_oliveira.beginnerspringbatchapp.domain.DailyAggregatedSensorData;
import me.calebe_oliveira.beginnerspringbatchapp.domain.DataAnomaly;
import org.springframework.batch.item.ItemProcessor;

public class SensorDataAnomalyProcessor implements ItemProcessor<DailyAggregatedSensorData, DataAnomaly> {
    private static final double THRESHOLD = 0.9;

    @Override
    public DataAnomaly process(DailyAggregatedSensorData item) throws Exception {
        if((item.getMin() / item.getAvg()) < THRESHOLD) {
            return new DataAnomaly(item.getDate(), AnomalyType.MINIMUM, item.getMin());
        } else if((item.getAvg() / item.getMax()) < THRESHOLD) {
            return new DataAnomaly(item.getDate(), AnomalyType.MAXIMUM, item.getMax());
        }

        return null;
    }
}
