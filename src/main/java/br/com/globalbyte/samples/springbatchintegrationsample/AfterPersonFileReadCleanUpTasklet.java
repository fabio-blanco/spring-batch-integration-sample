package br.com.globalbyte.samples.springbatchintegrationsample;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Tasklet that performs a cleanup of the files after processing.
 * It simply moves the input file to the processed directory.
 */
public class AfterPersonFileReadCleanUpTasklet implements Tasklet {

    /**
     * The input file path.
     */
    private String inputFilePath;

    /**
     * The path of the directory where the processed files should be stored.
     */
    private String processedDirPath;

    /**
     * The tasklet constructor that receives the paths to the input file and processed directory.
     *
     * @param inputFilePath The input file path.
     * @param processedDirPath The path of the directory where the processed files should be stored.
     */
    public AfterPersonFileReadCleanUpTasklet(String inputFilePath, String processedDirPath) {
        this.inputFilePath = inputFilePath;
        this.processedDirPath = processedDirPath;
    }

    /**
     * Executes the files cleanup.
     * It simply moves the input file to the processed directory.
     *
     * @param contribution mutable state to be passed back to update the current
     * step execution
     * @param chunkContext attributes shared between invocations but not between
     * restarts
     * @return a RepeatStatus indicating whether processing is continuable. Returning null is interpreted as RepeatStatus.FINISHED
     * @throws Exception thrown if error occurs during execution.
     */
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        Path filePath = Paths.get(inputFilePath);
        Path targetFilePath = Paths.get(processedDirPath, filePath.getFileName().toString());

        Files.move(filePath, targetFilePath);

        return RepeatStatus.FINISHED;
    }
}
