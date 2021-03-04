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

import com.google.common.io.Resources;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/** Test cases for the {@link FileFormatConversion} class. */
public class FixedWidthLoaderTest {
  @Rule public final transient TestPipeline mainPipeline = TestPipeline.create();

  @Rule public final transient TestPipeline readPipeline = TestPipeline.create();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Rule public final transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String RESOURCES_DIR = "FileFormatConversionTest/";

  private static final String CSV_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "csv_file.csv").getPath();

  private static final String CSV_FILE_WITH_MISSING_FIELD_PATH =
      Resources.getResource(RESOURCES_DIR + "missing_field.csv").getPath();

  private static final String AVRO_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "avro_file.avro").getPath();

  private static final String PARQUET_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "parquet_file.parquet").getPath();

  private static final String SCHEMA_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "avro_schema.json").getPath();

  private static final String SCHEMA_FILE_TWO_PATH =
      Resources.getResource(RESOURCES_DIR + "avro_schema_two.json").getPath();
}
