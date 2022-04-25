package br.com.globalbyte.samples.springbatchintegrationsample.integration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;

import java.io.File;
import java.nio.file.Paths;
import java.time.Instant;

/**
 * The transformer that transforms an input file message to a job request.
 */
public class FileMessageToJobRequest {
    /**
     * The job to be started.
     */
    private Job job;

    /**
     * The name of the job parameter that points to the input file name.
     */
    private String fileParameterName;

    /**
     * The name of the job parameter that points to the output file name.
     */
    private String outputFileParameterName;

    /**
     * The name of the job parameter that points to the directory where the processed files are stored.
     */
    private String processedDirParameterName;

    /**
     * The output directory path. Defaults to "./out".
     */
    private String outputDirPath = "./out";

    /**
     * The processed directory path. Defaults to "./processed".
     */
    private String processedDirPath = "./processed";

    /**
     * The input file extension. Defaults to "csv".
     */
    private String fileExtension = "csv";

    /**
     * Sets the input file extension.
     *
     * @param fileExtension The input file extension.
     */
    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    /**
     * Sets the processed dir path.
     *
     * @param processedDirPath The processed directory path.
     */
    public void setProcessedDirPath(String processedDirPath) {
        this.processedDirPath = processedDirPath;
    }

    /**
     * Sets the output dir path.
     *
     * @param outputDirPath he output directory path.
     */
    public void setOutputDirPath(String outputDirPath) {
        this.outputDirPath = outputDirPath;
    }

    /**
     * Sets the name of the job parameter that points to the directory where the processed files are stored.
     *
     * @param processedDirParameterName The name of the job parameter that points to the directory where the processed files are stored.
     */
    public void setProcessedDirParameterName(String processedDirParameterName) {
        this.processedDirParameterName = processedDirParameterName;
    }

    /**
     * Sets the name of the job parameter that points to the output file name.
     *
     * @param outputFileParameterName The name of the job parameter that points to the output file name.
     */
    public void setOutputFileParameterName(String outputFileParameterName) {
        this.outputFileParameterName = outputFileParameterName;
    }

    /**
     * Sets the name of the job parameter that points to the input file name.
     *
     * @param fileParameterName The name of the job parameter that points to the input file name.
     */
    public void setFileParameterName(String fileParameterName) {
        this.fileParameterName = fileParameterName;
    }

    /**
     * Sets the job to be started byt the job request.
     *
     * @param job The job to be started.
     */
    public void setJob(Job job) {
        this.job = job;
    }

    /**
     * The transformer method that transforms a file message into a job launch request for reading and processing this file.
     *
     * @param message A file message parameter.
     * @return The resulting job launch request.
     */
    @Transformer
    public JobLaunchRequest toRequest(Message<File> message) {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();

        long now = Instant.now().toEpochMilli();
        String filePath = message.getPayload().getAbsolutePath();
        String inputFileName = message.getPayload().getName();
        String outputFileName = inputFileName.replace("." + fileExtension, "-out-" + now + "." + fileExtension);
        String outputFilePath = Paths.get(outputDirPath, outputFileName).toAbsolutePath().toString();
        jobParametersBuilder.addString(fileParameterName, filePath);
        jobParametersBuilder.addString(outputFileParameterName, outputFilePath);
        jobParametersBuilder.addString(processedDirParameterName, processedDirPath);
        jobParametersBuilder.addLong("timestamp", now);

        return new JobLaunchRequest(job, jobParametersBuilder.toJobParameters());
    }
}
