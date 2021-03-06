package com.google.cloud.teleport.v2.templates;

import java.io.Serializable;

public class FixedWidthColumn implements Serializable {
  /** The {@link DataTypes} enum contains all valid datatypes. */
  public enum DataType {
    DATE,
    NUMERIC,
    STRING
  }

  private int offset;
  private int length;
  private String fieldName;
  private DataType type;
  private String format;

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public DataType getType() {
    return type;
  }

  public void setType(DataType type) {
    this.type = type;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public int endPosition() {
    return this.getOffset() + this.getLength();
  }
}

