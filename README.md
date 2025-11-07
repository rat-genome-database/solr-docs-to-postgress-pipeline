# Solr Docs to PostgreSQL Pipeline

This pipeline extracts Solr documents from JSON files and uploads them to a PostgreSQL database. It's optimized for high performance with batch processing and parallel execution.

## Overview

The pipeline is based on the `uploadToDB` method from `PubMedLibrary.java` in the pubmed-crawler2 project. It processes JSON files containing Solr documents and efficiently uploads them to the database while checking for duplicates.

## Features

- **Batch Processing**: Processes documents in configurable batches (default: 1000)
- **Parallel Execution**: Uses thread pool for concurrent processing (default: 10 threads)
- **Duplicate Detection**: Checks for existing documents before insertion
- **Progress Tracking**: Provides detailed statistics and progress logging
- **Error Handling**: Robust error handling with detailed logging
- **Configurable**: Command-line options for customization

## Prerequisites

- Java 11 or higher
- PostgreSQL database with RGD schema
- Access to RGD core library (edu.mcw.rgd:rgd-core-library)

## Building

Build the project using Gradle:

```bash
gradle build
```

This creates a fat JAR with all dependencies in `build/libs/solr-docs-to-postgress-pipeline-1.0.0.jar`

## Usage

### Basic Usage

```bash
java -jar build/libs/solr-docs-to-postgress-pipeline-1.0.0.jar -i /path/to/json/files
```

### Command Line Options

| Option | Long Option | Description | Required | Default |
|--------|------------|-------------|----------|---------|
| `-i` | `--input` | Input directory containing JSON files | Yes | - |
| `-b` | `--batch-size` | Number of documents per batch | No | 1000 |
| `-t` | `--threads` | Thread pool size | No | 10 |
| `--timeout` | - | Timeout in minutes | No | 30 |
| `-v` | `--verbose` | Enable verbose logging | No | false |
| `-h` | `--help` | Show help message | No | - |

### Examples

**Basic run with default settings:**
```bash
java -jar build/libs/solr-docs-to-postgress-pipeline-1.0.0.jar -i /data/solr-docs
```

**Custom batch size and thread pool:**
```bash
java -jar build/libs/solr-docs-to-postgress-pipeline-1.0.0.jar \
  -i /data/solr-docs \
  -b 2000 \
  -t 20
```

**Verbose logging:**
```bash
java -jar build/libs/solr-docs-to-postgress-pipeline-1.0.0.jar \
  -i /data/solr-docs \
  -v
```

**Using Gradle run task:**
```bash
gradle runPipeline --args="-i /data/solr-docs -b 1000 -t 10"
```

## Configuration

### Database Configuration

The pipeline uses the RGD core library's database configuration. Ensure your database connection is properly configured in your environment.

### Log4j2 Configuration

Logging is configured via `config/log4j2.xml`. Logs are written to:
- Console (stdout)
- File: `logs/solr-docs-pipeline.log` (rolling, max 10MB per file, 10 backup files with compression)

## Input Format

The pipeline expects JSON files where each line contains a single Solr document in JSON format:

```json
{"pmid":["12345678"],"title":["Example Title"],"abstract":["Example abstract text"],...}
{"pmid":["87654321"],"title":["Another Title"],"abstract":["Another abstract"],...}
```

## Output

The pipeline provides detailed statistics upon completion:

```
========================================
Pipeline Statistics:
========================================
Files processed:        10
Total documents:        50000
Processed (new):        45000
Skipped (existing):     5000
Errors:                 0
Processing time:        120.45 seconds
Throughput:             415 docs/sec
========================================
```

## Performance Tuning

- **Batch Size**: Increase for higher throughput, decrease for lower memory usage
- **Thread Pool**: Increase for better parallelism (recommended: number of CPU cores)
- **Timeout**: Adjust based on dataset size and expected processing time

## Project Structure

```
solr-docs-to-postgress-pipeline/
├── build.gradle                    # Gradle build configuration
├── settings.gradle                 # Gradle settings
├── config/
│   └── log4j.properties           # Logging configuration
├── logs/                          # Log files directory
├── src/
│   └── main/
│       ├── java/
│       │   └── edu/mcw/rgd/
│       │       ├── pipeline/solr/
│       │       │   └── SolrDocsToPostgresPipeline.java
│       │       └── process/
│       │           ├── MyThreadPoolExecutor.java
│       │           └── SolrDBProcessingThread.java
│       └── resources/
└── README.md
```

## Dependencies

- RGD Core Library (4.0.0)
- Jackson (2.15.2) - JSON processing
- PostgreSQL JDBC Driver (42.7.7)
- Apache Commons CLI (1.5.0)
- Log4j2 (2.20.0)

## Error Handling

- Malformed JSON lines are logged and skipped
- Database errors are logged with full stack traces
- Uncaught thread exceptions trigger graceful shutdown

## License

Copyright (c) Medical College of Wisconsin

## Contact

For issues or questions, contact the RGD development team.
