/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.bench;

import io.atomix.primitive.config.PrimitiveConfig;

import java.util.UUID;

/**
 * Benchmark configuration.
 */
public class BenchmarkConfig {
  private static final int DEFAULT_OPERATIONS = 10000;
  private static final int DEFAULT_WRITE_PERCENTAGE = 100;
  private static final int DEFAULT_NUM_KEYS = 100000;
  private static final int DEFAULT_KEY_LENGTH = 32;
  private static final int DEFAULT_NUM_UNIQUE_VALUES = 100;
  private static final int DEFAULT_VALUE_LENGTH = 1024;
  private static final boolean DEFAULT_INCLUDE_EVENTS = false;
  private static final boolean DEFAULT_DETERMINISTIC = true;

  private String benchId = UUID.randomUUID().toString();
  private PrimitiveConfig primitive;
  private int operations = DEFAULT_OPERATIONS;
  private int writePercentage = DEFAULT_WRITE_PERCENTAGE;
  private int numKeys = DEFAULT_NUM_KEYS;
  private int keyLength = DEFAULT_KEY_LENGTH;
  private int numValues = DEFAULT_NUM_UNIQUE_VALUES;
  private int valueLength = DEFAULT_VALUE_LENGTH;
  private boolean includeEvents = DEFAULT_INCLUDE_EVENTS;
  private boolean deterministic = DEFAULT_DETERMINISTIC;

  public String getBenchId() {
    return benchId;
  }

  public BenchmarkConfig setBenchId(String benchId) {
    this.benchId = benchId;
    return this;
  }

  public PrimitiveConfig getPrimitive() {
    return primitive;
  }

  public BenchmarkConfig setPrimitive(PrimitiveConfig primitive) {
    this.primitive = primitive;
    return this;
  }

  public int getOperations() {
    return operations;
  }

  public BenchmarkConfig setOperations(int operations) {
    this.operations = operations;
    return this;
  }

  public int getWritePercentage() {
    return writePercentage;
  }

  public BenchmarkConfig setWritePercentage(int writePercentage) {
    this.writePercentage = writePercentage;
    return this;
  }

  public int getNumKeys() {
    return numKeys;
  }

  public BenchmarkConfig setNumKeys(int numKeys) {
    this.numKeys = numKeys;
    return this;
  }

  public int getKeyLength() {
    return keyLength;
  }

  public BenchmarkConfig setKeyLength(int keyLength) {
    this.keyLength = keyLength;
    return this;
  }

  public int getNumValues() {
    return numValues;
  }

  public BenchmarkConfig setNumValues(int numValues) {
    this.numValues = numValues;
    return this;
  }

  public int getValueLength() {
    return valueLength;
  }

  public BenchmarkConfig setValueLength(int valueLength) {
    this.valueLength = valueLength;
    return this;
  }

  public boolean isIncludeEvents() {
    return includeEvents;
  }

  public BenchmarkConfig setIncludeEvents(boolean includeEvents) {
    this.includeEvents = includeEvents;
    return this;
  }

  public boolean isDeterministic() {
    return deterministic;
  }

  public BenchmarkConfig setDeterministic(boolean deterministic) {
    this.deterministic = deterministic;
    return this;
  }
}
