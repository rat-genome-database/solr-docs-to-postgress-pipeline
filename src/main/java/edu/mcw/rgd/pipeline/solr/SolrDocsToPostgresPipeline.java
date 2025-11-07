package edu.mcw.rgd.pipeline.solr;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import edu.mcw.rgd.dao.impl.solr.SolrDocsDAO;
import edu.mcw.rgd.datamodel.solr.SolrDoc;
import edu.mcw.rgd.process.MyThreadPoolExecutor;
import edu.mcw.rgd.process.SolrDBProcessingThread;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Pipeline to upload Solr documents from JSON files to PostgreSQL database
 * Optimized for high performance with batch processing and parallel execution
 *
 * @author Jthota
 * @version 1.0.0
 */
public class SolrDocsToPostgresPipeline {

    private static final Logger logger = Logger.getLogger(SolrDocsToPostgresPipeline.class);
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int DEFAULT_THREAD_POOL_SIZE = 10;
    private static final int DEFAULT_TIMEOUT_MINUTES = 30;

    private String inputDirectory;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    private int timeoutMinutes = DEFAULT_TIMEOUT_MINUTES;
    private boolean verbose = false;

    public static void main(String[] args) {
        try {
            // Configure logging
            PropertyConfigurator.configure("config/log4j.properties");

            // Parse command line arguments
            SolrDocsToPostgresPipeline pipeline = new SolrDocsToPostgresPipeline();
            if (!pipeline.parseArguments(args)) {
                System.exit(1);
            }

            // Run the pipeline
            logger.info("Starting Solr Docs to Postgres Pipeline");
            long startTime = System.currentTimeMillis();

            pipeline.execute();

            long duration = System.currentTimeMillis() - startTime;
            logger.info(String.format("Pipeline completed in %.2f seconds", duration / 1000.0));

        } catch (Exception e) {
            logger.error("Fatal error in pipeline execution", e);
            System.exit(1);
        }
    }

    /**
     * Parse command line arguments
     */
    private boolean parseArguments(String[] args) {
        Options options = new Options();

        options.addOption(Option.builder("i")
                .longOpt("input")
                .hasArg()
                .required()
                .desc("Input directory containing JSON files")
                .build());

        options.addOption(Option.builder("b")
                .longOpt("batch-size")
                .hasArg()
                .desc("Batch size for processing (default: 1000)")
                .build());

        options.addOption(Option.builder("t")
                .longOpt("threads")
                .hasArg()
                .desc("Thread pool size (default: 10)")
                .build());

        options.addOption(Option.builder("timeout")
                .hasArg()
                .desc("Timeout in minutes (default: 30)")
                .build());

        options.addOption(Option.builder("v")
                .longOpt("verbose")
                .desc("Enable verbose logging")
                .build());

        options.addOption(Option.builder("h")
                .longOpt("help")
                .desc("Print this help message")
                .build());

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                printHelp(options);
                return false;
            }

            this.inputDirectory = cmd.getOptionValue("i");

            if (cmd.hasOption("b")) {
                this.batchSize = Integer.parseInt(cmd.getOptionValue("b"));
            }

            if (cmd.hasOption("t")) {
                this.threadPoolSize = Integer.parseInt(cmd.getOptionValue("t"));
            }

            if (cmd.hasOption("timeout")) {
                this.timeoutMinutes = Integer.parseInt(cmd.getOptionValue("timeout"));
            }

            this.verbose = cmd.hasOption("v");

            // Validate input directory
            File dir = new File(this.inputDirectory);
            if (!dir.exists() || !dir.isDirectory()) {
                logger.error("Input directory does not exist or is not a directory: " + this.inputDirectory);
                return false;
            }

            return true;

        } catch (ParseException e) {
            logger.error("Error parsing command line arguments: " + e.getMessage());
            printHelp(options);
            return false;
        }
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("solr-docs-to-postgres-pipeline", options);
    }

    /**
     * Execute the pipeline
     */
    public void execute() throws Exception {
        ExecutorService executor = null;
        SolrDocsDAO solrDocsDAO = null;

        try {
            File folder = new File(inputDirectory);
            File[] files = folder.listFiles((dir, name) -> name.endsWith(".json"));

            if (files == null || files.length == 0) {
                logger.warn("No JSON files found in directory: " + inputDirectory);
                return;
            }

            logger.info(String.format("Found %d JSON files to process", files.length));
            logger.info(String.format("Configuration: batch_size=%d, threads=%d, timeout=%d minutes",
                    batchSize, threadPoolSize, timeoutMinutes));

            ObjectMapper mapper = configureObjectMapper();
            List<SolrDoc> solrDocs = new ArrayList<>();
            List<Integer> chunkDataCounts = new ArrayList<>();

            // Reuse single executor and DAO across all files
            executor = new MyThreadPoolExecutor(threadPoolSize, threadPoolSize, 0L,
                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            solrDocsDAO = new SolrDocsDAO();

            PipelineStatistics stats = new PipelineStatistics();

            for (File fileEntry : files) {
                if (fileEntry == null || !fileEntry.isFile()) {
                    continue;
                }

                try {
                    logger.info("Processing file: " + fileEntry.getName());

                    try (BufferedReader objReader = new BufferedReader(new FileReader(fileEntry))) {
                        String strCurrentLine;
                        int linesRead = 0;

                        while ((strCurrentLine = objReader.readLine()) != null) {
                            linesRead++;
                            try {
                                SolrDoc doc = mapper.readValue(strCurrentLine, SolrDoc.class);
                                solrDocs.add(doc);

                                // Process in batches
                                if (solrDocs.size() >= batchSize) {
                                    List<SolrDoc> batch = new ArrayList<>(solrDocs);
                                    BatchResult result = processBatch(batch, executor, chunkDataCounts, solrDocsDAO);
                                    stats.addBatchResult(result);
                                    solrDocs.clear();

                                    if (verbose && linesRead % 10000 == 0) {
                                        logger.info(String.format("  Processed %d lines from %s", linesRead, fileEntry.getName()));
                                    }
                                }
                            } catch (Exception e) {
                                logger.error(String.format("Error parsing line %d in file %s", linesRead, fileEntry.getName()), e);
                                stats.incrementErrors();
                            }
                        }

                        // Process remaining documents
                        if (!solrDocs.isEmpty()) {
                            List<SolrDoc> batch = new ArrayList<>(solrDocs);
                            BatchResult result = processBatch(batch, executor, chunkDataCounts, solrDocsDAO);
                            stats.addBatchResult(result);
                            solrDocs.clear();
                        }

                        stats.incrementFilesProcessed();
                    }

                    logger.info("Completed file: " + fileEntry.getName());

                } catch (Exception e) {
                    logger.error("Error processing file: " + fileEntry.getName(), e);
                    stats.incrementErrors();
                }
            }

            // Shutdown executor and wait for completion
            if (executor != null) {
                logger.info("Waiting for all threads to complete...");
                executor.shutdown();
                boolean terminated = executor.awaitTermination(timeoutMinutes, TimeUnit.MINUTES);

                if (!terminated) {
                    logger.warn("Executor did not terminate within timeout period, forcing shutdown");
                    executor.shutdownNow();
                }
            }

            // Print final statistics
            stats.printStatistics();

        } finally {
            // Ensure executor is shutdown
            if (executor != null && !executor.isShutdown()) {
                executor.shutdownNow();
            }
        }
    }

    /**
     * Configure Jackson ObjectMapper for JSON deserialization
     */
    private ObjectMapper configureObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.setVisibility(VisibilityChecker.Std.defaultInstance()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY));
        return mapper;
    }

    /**
     * Process a batch of documents - check existence in batch and submit non-existing ones
     *
     * @param batch           List of documents to process
     * @param executor        Executor service for parallel processing
     * @param chunkDataCounts List to track chunk data counts
     * @param solrDocsDAO     DAO for database operations
     * @return BatchResult containing processing statistics
     */
    private BatchResult processBatch(List<SolrDoc> batch, ExecutorService executor,
                                      List<Integer> chunkDataCounts, SolrDocsDAO solrDocsDAO) throws Exception {
        BatchResult result = new BatchResult();
        result.totalDocs = batch.size();

        // Extract PMIDs for batch existence check
        List<String> pmids = batch.stream()
                .filter(doc -> doc.getPmid() != null && !doc.getPmid().isEmpty())
                .map(doc -> doc.getPmid().get(0))
                .collect(Collectors.toList());

        // Batch check for existing documents
        Set<String> existingPmids = solrDocsDAO.getExistingPmids(pmids);

        // Filter out existing documents
        List<SolrDoc> newDocs = batch.stream()
                .filter(doc -> doc.getPmid() != null && !doc.getPmid().isEmpty()
                        && !existingPmids.contains(doc.getPmid().get(0)))
                .collect(Collectors.toList());

        result.processedDocs = newDocs.size();
        result.skippedDocs = batch.size() - newDocs.size();

        // Submit non-existing documents for processing
        if (!newDocs.isEmpty()) {
            Runnable workerThread = new SolrDBProcessingThread(newDocs, chunkDataCounts);
            executor.execute(workerThread);
        }

        return result;
    }

    /**
     * Statistics tracking for batch processing
     */
    private static class BatchResult {
        int totalDocs = 0;
        int processedDocs = 0;
        int skippedDocs = 0;
    }

    /**
     * Overall pipeline statistics
     */
    private static class PipelineStatistics {
        private int filesProcessed = 0;
        private int totalDocs = 0;
        private int processedDocs = 0;
        private int skippedDocs = 0;
        private int errors = 0;
        private long startTime = System.currentTimeMillis();

        void addBatchResult(BatchResult result) {
            totalDocs += result.totalDocs;
            processedDocs += result.processedDocs;
            skippedDocs += result.skippedDocs;
        }

        void incrementFilesProcessed() {
            filesProcessed++;
        }

        void incrementErrors() {
            errors++;
        }

        void printStatistics() {
            long duration = System.currentTimeMillis() - startTime;
            double seconds = duration / 1000.0;

            logger.info("========================================");
            logger.info("Pipeline Statistics:");
            logger.info("========================================");
            logger.info(String.format("Files processed:        %d", filesProcessed));
            logger.info(String.format("Total documents:        %d", totalDocs));
            logger.info(String.format("Processed (new):        %d", processedDocs));
            logger.info(String.format("Skipped (existing):     %d", skippedDocs));
            logger.info(String.format("Errors:                 %d", errors));
            logger.info(String.format("Processing time:        %.2f seconds", seconds));

            if (totalDocs > 0) {
                logger.info(String.format("Throughput:             %.0f docs/sec", totalDocs / seconds));
            }

            logger.info("========================================");
        }
    }
}
