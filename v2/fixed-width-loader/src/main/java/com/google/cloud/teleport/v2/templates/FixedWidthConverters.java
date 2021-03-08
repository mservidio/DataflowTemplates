/*
 * Copyright (C) 2021 Google Inc.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.FixedWidthColumn.DataType;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.gson.JsonObject;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.AvroIO.Parse;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common transforms for Fixed width files. */
public class FixedWidthConverters {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(FixedWidthConverters.class);

  static List<FixedWidthColumn> getFileDefinition(String path) {
    String fileDefinitionContent = SchemaUtils.getGcsFileAsString(path);
    LOG.info(fileDefinitionContent);
    ObjectMapper mapper = new ObjectMapper();
    try {
      List<FixedWidthColumn> list = mapper.readValue(fileDefinitionContent, new TypeReference<List<FixedWidthColumn>>() {});
      LOG.info("File definition deserialized");
      return list;
    } catch (JsonProcessingException ex) {
      LOG.error(ex.getMessage() + " - " + ex.getStackTrace());
      return null;
    }
  }

  static class FixedWidthParsingFn extends DoFn<String, String> {
    String definitionPath;
    List<FixedWidthColumn> definition;

    public FixedWidthParsingFn(String definitionPath) {
      this.definitionPath = definitionPath;
    }

    @Setup
    public void setup() {
      List<FixedWidthColumn> d = getFileDefinition(this.definitionPath);
      this.definition = d.stream()
          .sorted(Comparator.comparingInt(FixedWidthColumn::getOffset))
          .collect(Collectors.toList());
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      String line = c.element();
      int length = line.length();

      JsonObject json = new JsonObject();
      for (FixedWidthColumn i : this.definition) {
        if (i.getOffset() < length - 1) {
          try {
            String strValue = line.substring(i.getOffset(), i.endPosition());
            String outValue = castAsString(i, strValue);
            json.addProperty(i.getFieldName(), outValue);
          } catch (ParseException ex) {
            // Add failsafe logic
            json.addProperty(i.getFieldName(), "");
          }
        } else {
          // Error condition
        }
      }
      String j = json.toString();
      LOG.info(j);
      c.output(json.toString());
    }

    private String castAsString(FixedWidthColumn field, String str) throws ParseException {
      switch (field.getType()) {
        case DATE:
          SimpleDateFormat formatter = new SimpleDateFormat(field.getFormat());
          Date date = formatter.parse(str);

          SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          String dateStr = formatter2.format(date);
          return dateStr;

        case STRING:
          return str;

        case NUMERIC:
          Number number = NumberFormat.getInstance().parse(str);
          String numStr = String.valueOf(number);
          return numStr;

        default:
          LOG.error("Invalid data type, got: " + field.getType());
          throw new RuntimeException("Invalid data type, got: " + field.getType());
      }
    }
  }
}
