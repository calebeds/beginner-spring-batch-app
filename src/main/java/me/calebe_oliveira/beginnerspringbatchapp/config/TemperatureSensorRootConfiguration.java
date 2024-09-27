package me.calebe_oliveira.beginnerspringbatchapp.config;

import me.calebe_oliveira.beginnerspringbatchapp.domain.DailyAggregatedSensorData;
import me.calebe_oliveira.beginnerspringbatchapp.domain.DailySensorData;
import me.calebe_oliveira.beginnerspringbatchapp.domain.DataAnomaly;
import me.calebe_oliveira.beginnerspringbatchapp.mapper.SensorDataTextMapper;
import me.calebe_oliveira.beginnerspringbatchapp.processors.RawToAggregateSensorDataProcessor;
import me.calebe_oliveira.beginnerspringbatchapp.processors.SensorDataAnomalyProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class TemperatureSensorRootConfiguration {
    @Value("classpath:input/HTE2NP.txt")
    private Resource rawDailyInputResource;
    @Value("classpath:output/HTE2NP.xml")
    private WritableResource aggregatedDailyOutputXmlResource;
    @Value("classpath:output/HTE2NP-anomalies.csv")
    private WritableResource anomalyDataResource;

    @Bean
    public Job temperatureSensorJob(JobRepository jobRepository,
                                    @Qualifier("aggregateSensorStep")Step aggreateSensorStep,
                                    @Qualifier("reportAnomaliesStep") Step reportAnomaliesStep) {
        return new JobBuilder("temperatureSensorJob", jobRepository)
                .start(aggreateSensorStep)
                .next(reportAnomaliesStep)
                .build();
    }

    @Bean
    @Qualifier("aggregateSensorStep")
    public Step aggregateSensorStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("aggregate-sensor", jobRepository)
                .<DailySensorData, DailyAggregatedSensorData>chunk(1, transactionManager)
                .reader(new FlatFileItemReaderBuilder<DailySensorData>()
                        .name("dailySensorDataReader")
                        .resource(rawDailyInputResource)
                        .lineMapper(new SensorDataTextMapper())
                        .build())
                .processor(new RawToAggregateSensorDataProcessor())
                .writer(new StaxEventItemWriterBuilder<DailyAggregatedSensorData>()
                        .name("dailyAggregatedSensorDataWriter")
                        .marshaller(DailyAggregatedSensorData.getMarshaler())
                        .resource(aggregatedDailyOutputXmlResource)
                        .rootTagName("data")
                        .overwriteOutput(true)
                        .build())
                .build();

    }

    @Bean
    @Qualifier("reportAnomaliesStep")
    public Step reportAnomaliesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("report-anomalies", jobRepository)
                .<DailyAggregatedSensorData, DataAnomaly>chunk(1, transactionManager)
                .reader(new StaxEventItemReaderBuilder<DailyAggregatedSensorData>()
                        .name("dailyAggregatedSensorDataReader")
                        .unmarshaller(DailyAggregatedSensorData.getMarshaler())
                        .resource(aggregatedDailyOutputXmlResource)
                        .addFragmentRootElements(DailyAggregatedSensorData.ITEM_ROOT_ELEMENT_NAME)
                        .build())
                .processor(new SensorDataAnomalyProcessor())
                .writer(new FlatFileItemWriterBuilder<DataAnomaly>()
                        .name("dataAnomalyWriter")
                        .resource(anomalyDataResource)
                        .delimited()
                        .delimiter(",")
                        .names(new String[] {"date", "type", "value"})
                        .build()
                )
                .build();
    }

    @Bean
    public DataSource dataSource(@Value("${db.driverClassName}") String driverClassName,
                                 @Value("${db.url}") String url,
                                 @Value("${db.username}") String username,
                                 @Value("${db.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        JdbcTransactionManager transactionManager = new JdbcTransactionManager();
        transactionManager.setDataSource(dataSource);
        return transactionManager;
    }

    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceInitializer(DataSource dataSource,
                                                                               BatchProperties properties) {
        return new BatchDataSourceScriptDatabaseInitializer(dataSource, properties.getJdbc());
    }

    public BatchProperties batchProperties(@Value("${batch.db.initialize-schema}") DatabaseInitializationMode initializationMode) {
        BatchProperties properties = new BatchProperties();
        properties.getJdbc().setInitializeSchema(initializationMode);
        return properties;
    }
}
