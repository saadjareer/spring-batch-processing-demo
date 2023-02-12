package com.example.sjdemospringbatch;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Properties;


@EnableAutoConfiguration
@EnableBatchProcessing(dataSourceRef = "batchDataSource", transactionManagerRef = "batchTransactionManager")
public class SjDemoSpringBatchApplication {

    @Bean
    public DataSource batchDataSource() {
        return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2)
                .addScript("/org/springframework/batch/core/schema-h2.sql")
                .generateUniqueName(false).build();
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource());
        em.setPackagesToScan(new String[] {"com.example.sjdemospringbatch"});
        JpaVendorAdapter jpaAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(jpaAdapter);
        em.setJpaProperties(jpaProperties());

        return em;
    }

    private final Properties jpaProperties() {
        Properties properties = new Properties();
        properties.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL5Dialect");

        return properties;
    }

    @Bean
    public DataSource dataSource() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.url("jdbc:mysql://localhost:3306/Test");
        dataSourceBuilder.username("root");
        dataSourceBuilder.password("root-password");
        return dataSourceBuilder.build();
    }

    @Bean
    public JdbcTransactionManager batchTransactionManager(DataSource batchDataSource) {
        return new JdbcTransactionManager(batchDataSource);
    }

    public static void main(String e []) throws Exception {

        ApplicationContext context = new AnnotationConfigApplicationContext(SjDemoSpringBatchApplication.class);

        JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis()).toJobParameters();


        JobLauncher jobLauncher = context.getBean(JobLauncher.class);

        JobExecution jobExecution = jobLauncher.run(context.getBean(Job.class), jobParameters);
        System.out.println("Job Exit Status : " + jobExecution.getStatus());
    }

    @Bean
    public Step step(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step", jobRepository)
                .<Employee, Employee>chunk(5, transactionManager)
                .reader(itemReader())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public JdbcTransactionManager transactionManager(DataSource dataSource) {
        return new JdbcTransactionManager(dataSource);
    }

    @Bean
    public Job job(JobRepository jobRepository, JdbcTransactionManager transactionManager) {
        return new JobBuilder("job", jobRepository)
                .start(step(jobRepository, transactionManager))
                .build();
    }

    @Bean
    public JdbcCursorItemReader<Employee> itemReader() {
        String sql = "select * from person";
        return new JdbcCursorItemReaderBuilder<Employee>()
                .name("personItemReader")
                .dataSource(dataSource())
                .sql(sql)
                .beanRowMapper(Employee.class)
                .build();
    }

    @Bean
    public FlatFileItemWriter<Employee> itemWriter() {
        return new FlatFileItemWriterBuilder<Employee>()
                .resource(new FileSystemResource("persons.csv"))
                .name("personItemWriter")
                .delimited()
                .names("id", "name")
                .build();
    }

}
