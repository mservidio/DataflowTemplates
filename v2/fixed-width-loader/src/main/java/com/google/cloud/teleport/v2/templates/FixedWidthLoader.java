/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.templates;

import java.util.EnumMap;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link FileFormatConversion} pipeline takes in an input file, converts it to a desired format
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
   * The {@link FileFormatConversionOptions} provides the custom execution options passed by the
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

    Pipeline pipline = Pipeline.create(options);

    PCollection<String> lines = pipline.apply(
        "ReadLines", TextIO.read().from(options.getInputFilePattern()));

    PCollection formatted = lines.apply(
        "Converting to String",
        ParDo.of(
            new DoFn<String, String>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                c.output(c.element());
              }
            }));

    formatted.apply("Write File(s)",
        TextIO.write().to("gs://fixed-width-template/files/1_out.txt"));

    return pipline.run();
  }
}
