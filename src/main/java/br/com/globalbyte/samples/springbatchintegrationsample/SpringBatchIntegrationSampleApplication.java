package br.com.globalbyte.samples.springbatchintegrationsample;

import br.com.globalbyte.samples.springbatchintegrationsample.domain.Person;
import br.com.globalbyte.samples.springbatchintegrationsample.integration.FileMessageToJobRequest;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.handler.LoggingHandler;

import java.io.File;

/**
 * The Spring Batch Integration sample Application.
 */
@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchIntegrationSampleApplication {

    /**
     * The job builder factory.
     */
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    /**
     * The step builder factory.
     */
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    /**
     * The job repository.
     */
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private JobRepository jobRepository;

    /**
     * Main Spring Boot entry point for bootstrapping this app.
     *
     * @param args The list of args for the main. (ignored)
     */
    public static void main(String[] args) {
        SpringApplication.run(SpringBatchIntegrationSampleApplication.class, args);
    }

    /**
     * Builds The transformer that will be used by the integration flow to transform each file message obtained from the poller inbound adapter
     * to a Spring Batch job request.
     *
     * @return The transformer of file message to job request.
     */
    @Bean
    public FileMessageToJobRequest fileMessageToJobRequest() {
        FileMessageToJobRequest fileMessageToJobRequest = new FileMessageToJobRequest();
        fileMessageToJobRequest.setFileParameterName("input.file.name");
        fileMessageToJobRequest.setOutputFileParameterName("output.file.name");
        fileMessageToJobRequest.setProcessedDirParameterName("processed.dir.path");
        fileMessageToJobRequest.setFileExtension("csv");
        fileMessageToJobRequest.setOutputDirPath("./test_files/output");
        fileMessageToJobRequest.setProcessedDirPath("./test_files/processed");
        fileMessageToJobRequest.setJob(readSimpleFileJob());
        return fileMessageToJobRequest;
    }

    /**
     * The gateway used to launch batch jobs.
     *
     * @return The gateway used to launch batch jobs.
     */
    @Bean
    public JobLaunchingGateway jobLaunchingGateway() {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        simpleJobLauncher.setTaskExecutor(new SyncTaskExecutor());
        JobLaunchingGateway jobLaunchingGateway = new JobLaunchingGateway(simpleJobLauncher);

        return jobLaunchingGateway;
    }

    /**
     * The integration flow that polls the csv files in the input directory, wrap them into job requests and start those jobs.
     * This is the core of the integration in this sample application, it bootstraps the batch jobs that will process the files polled
     * from the input directory. <br>
     * Since this is the main source of job starts, the property <code>spring.batch.job.enabled</code> is set to <code>false</code> int the
     * <strong>application.properties</strong> file.
     *
     * @param jobLaunchingGateway The job launcher gateway used to handle the starting of the job.
     * @return The main integration flow of this app.
     */
    @Bean
    public IntegrationFlow integrationFlow(JobLaunchingGateway jobLaunchingGateway) {
        return IntegrationFlows.from(Files.inboundAdapter(new File("./test_files/input")).filter(new SimplePatternFileListFilter("*.csv")),
                                     c -> c.poller(Pollers.fixedRate(1000).maxMessagesPerPoll(1))).
                               transform(fileMessageToJobRequest()).
                               handle(jobLaunchingGateway).
                               log(LoggingHandler.Level.WARN, "headers.id + ': ' + payload").
                               get();
    }

    /**
     * A job that processes csv person files.
     *
     * @return A job that processes csv person files.
     */
    @Bean
    public Job readSimpleFileJob() {
        return jobBuilderFactory.get("readSimpleFileJob")
                                .start(personFileStep())
                                .next(personFileCleanUpStep())
                                .build();
    }

    /**
     * A step with a cleanup tasklet to be called after a file is processed.
     *
     * @return The person's file cleanup step.
     */
    @Bean
    public Step personFileCleanUpStep() {
        return stepBuilderFactory.get("personFileCleanUpStep")
                                 .tasklet(afterPersonFileReadCleanUpTasklet(null, null))
                                 .build();
    }

    /**
     * The step that reads a person file and processes it generating the output upper-cased persons file.
     *
     * @return The persons file step
     */
    @Bean
    public Step personFileStep() {
        return stepBuilderFactory.get("personFileStep")
                                 .<Person, Person> chunk(10)
                                 .reader(reader(null))
                                 .processor(processor())
                                 .writer(writer(null))
                                 .build();
    }

    /**
     * The reader that reads persons from the csv comma delimited files.
     *
     * @param resource The input file name from the job parameters.
     * @return The reader that reads persons from the csv comma delimited files.
     */
    @Bean
    @StepScope
    public FlatFileItemReader<Person> reader(@Value("#{jobParameters['input.file.name']}") String resource) {
        return new FlatFileItemReaderBuilder<Person>().name("personItemReader")
                                                      .resource(new FileSystemResource(resource))
                                                      .delimited()
                                                      .names("firstName", "lastName")
                                                      .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                                                          setTargetType(Person.class);
                                                      }})
                                                      .build();
    }

    /**
     * The processor that upper-cases the person names.
     *
     * @return The processor that upper-cases the person names.
     */
    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    /**
     * The writer that write the upper-cased persons to the output file.
     *
     * @param resource The output file name from the job parameters.
     * @return The writer that write the upper-cased persons to the output file.
     */
    @Bean
    @StepScope
    public FlatFileItemWriter<Person> writer(@Value("#{jobParameters['output.file.name']}") String resource) {
        return new FlatFileItemWriterBuilder<Person>().name("personItemWriter")
                                                      .resource(new FileSystemResource(resource))
                                                      .delimited()
                                                      .names("firstName", "lastName")
                                                      .build();
    }

    /**
     * The tasklet that performs the files cleanup after the processing.
     *
     * @param inputFileName The input file name.
     * @param processedDirName The output file name.
     * @return The tasklet that performs the files cleanup after the processing.
     */
    @Bean
    @StepScope
    public Tasklet afterPersonFileReadCleanUpTasklet(@Value("#{jobParameters['input.file.name']}") String inputFileName,
                                                     @Value("#{jobParameters['processed.dir.path']}") String processedDirName) {
        return new AfterPersonFileReadCleanUpTasklet(inputFileName, processedDirName);
    }

}
