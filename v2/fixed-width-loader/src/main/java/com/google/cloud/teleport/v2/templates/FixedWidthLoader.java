/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.templates.FixedWidthColumn;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link FixedWidthLoader} pipeline takes in an input file, converts it to a desired format
 * and saves it to Cloud Storage. Supported file transformations are:
 *
 * <ul>
 *   <li>Csv to Avro
 *   <li>Csv to Parquet
 *   <li>Avro to Parquet
 *   <li>Parquet to Avro
 * </ul>
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>Input file exists in Google Cloud Storage.
 *   <li>Google Cloud Storage output bucket exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT=my-project
 * BUCKET_NAME=my-bucket
 *
 * # Set containerization vars
 * IMAGE_NAME=my-image-name
 * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 * BASE_CONTAINER_IMAGE=my-base-container-image
 * BASE_CONTAINER_IMAGE_VERSION=my-base-container-image-version
 * APP_ROOT=/path/to/app-root
 * COMMAND_SPEC=/path/to/command-spec
 *
 * # Set vars for execution
 * export INPUT_FILE_FORMAT=Csv
 * export OUTPUT_FILE_FORMAT=Avro
 * export AVRO_SCHEMA_PATH=gs://path/to/avro/schema
 * export HEADERS=false
 * export DELIMITER=","
 *
 * # Build and upload image
 * mvn clean package \
 * -Dimage=${TARGET_GCR_IMAGE} \
 * -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
 * -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
 * -Dapp-root=${APP_ROOT} \
 * -Dcommand-spec=${COMMAND_SPEC}
 *
 * # Create an image spec in GCS that contains the path to the image
 * {
 *    "docker_template_spec": {
 *       "docker_image": $TARGET_GCR_IMAGE
 *     }
 *  }
 *
 * # Execute template:
 * API_ROOT_URL="https://dataflow.googleapis.com"
 * TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
 * JOB_NAME="csv-to-avro-`date +%Y%m%d-%H%M%S-%N`"
 *
 * time curl -X POST -H "Content-Type: application/json"     \
 *     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 *     "${TEMPLATES_LAUNCH_API}"`
 *     `"?validateOnly=false"`
 *     `"&dynamicTemplate.gcsPath=${BUCKET_NAME}/path/to/image-spec"`
 *     `"&dynamicTemplate.stagingLocation=${BUCKET_NAME}/staging" \
 *     -d '
 *      {
 *       "jobName":"'$JOB_NAME'",
 *       "parameters": {
 *            "inputFileFormat":"'$INPUT_FILE_FORMAT'",
 *            "outputFileFormat":"'$OUTPUT_FILE_FORMAT'",
 *            "inputFileSpec":"'$BUCKET_NAME/path/to/input-file'",
 *            "outputBucket":"'$BUCKET_NAME/path/to/output-location/'",
 *            "containsHeaders":"'$HEADERS'",
 *            "schema":"'$AVRO_SCHEMA_PATH'",
 *            "outputFilePrefix":"output-file",
 *            "numShards":"3",
 *            "delimiter":"'$DELIMITER'"
 *         }
 *       }
 *      '
 * </pre>
 */
public class FixedWidthLoader {

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(FixedWidthLoader.class);

  private static EnumMap<ValidDestinations, String> validDestinations =
      new EnumMap<ValidDestinations, String>(ValidDestinations.class);

  /**
   * The {@link FixedWidthFormatConversionOptions} provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface FixedWidthFormatConversionOptions
      extends PipelineOptions {
    @Description("The GCS location of the text to process")
    @Required
    String getInputFilePattern();

    void setInputFilePattern(String inputFilePattern);

    @Description("The GCS location of the fixed width file definition")
    @Required
    String getFileDefinition();

    void setFileDefinition(String fileDefinition);

    @Description("Output destination")
    @Required
    String getOutputDestination();

    void setOutputDestination(String outputDestination);

    @Description("Header lines to skip")
    int getSkipCount();

    void setSkipCount(int skipCount);

    @Description(
        "The Cloud Storage file output path.")
    String getOutputFilePath();

    void setOutputFilePath(String outputFilePath);

    @Description(
        "The Cloud Pub/Sub topic to publish to. "
            + "The name should be in the format of "
            + "projects/<project-id>/topics/<topic-name>.")
    String getOutputTopic();

    void setOutputTopic(String outputTopic);

    @Description("Table spec to write the output to")
    @Required
    String getOutputTableSpec();

    void setOutputTableSpec(String outputTableSpec);

    @Description(
        "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
            + "format. If it doesn't exist, it will be created during pipeline execution.")
    String getOutputDeadletterTable();

    void setOutputDeadletterTable(String value);
  }

  /** The {@link ValidDestinations} enum contains all valid destinations. */
  public enum ValidDestinations {
    BIG_QUERY,
    CLOUD_STORAGE,
    PUB_SUB
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    FixedWidthFormatConversionOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(FixedWidthFormatConversionOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options.
   *
   * @param options The execution options.
   * @return The pipeline result.
   * @throws RuntimeException thrown if incorrect file formats are passed.
   */
  public static PipelineResult run(FixedWidthFormatConversionOptions options) {
    String outputDestination = options.getOutputDestination().toUpperCase();

    validDestinations.put(ValidDestinations.BIG_QUERY, "BIG_QUERY");
    validDestinations.put(ValidDestinations.CLOUD_STORAGE, "CLOUD_STORAGE");
    validDestinations.put(ValidDestinations.PUB_SUB, "PUB_SUB");

    try {
      if (!validDestinations.containsValue(options.getOutputDestination())) {
        LOG.error("Invalid output destination.");
        throw new IOException();
      }
    } catch (IOException e) {
      throw new RuntimeException("Provide correct output destination.");
    }

    ValidDestinations destination = ValidDestinations.valueOf(options.getOutputDestination());

    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps:
     *  1) Read messages in from fixed width file
     *  2) Transform the messages into TableRows
     *  3) Write successful records out to BigQuery
     *  4) Write failed records out to BigQuery
     */

    PCollection<String> lines = pipeline.apply(
        "ReadLines", TextIO.read().from(options.getInputFilePattern()));

    PCollection formatted = lines.apply(
        "Converting Fixed Width to JSON",
        ParDo.of(new FixedWidthConverters.FixedWidthParsingFn(options.getFileDefinition())));

    if (destination.equals(ValidDestinations.CLOUD_STORAGE)) {
      formatted.apply("Write File(s)",
          TextIO.write().to(options.getOutputFilePath()));
    } else if (destination.equals(ValidDestinations.BIG_QUERY)) {
      // BQ
    } else if (destination.equals(ValidDestinations.PUB_SUB)) {
      formatted.apply("writeSuccessMessages", PubsubIO.writeStrings().to(options.getOutputTopic()));
    }

    return pipeline.run();
  }
}
