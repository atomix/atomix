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
package io.atomix.bench.map;

import io.atomix.bench.BenchmarkConfig;
import io.atomix.bench.BenchmarkType;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;

/**
 * Map benchmark configuration.
 */
public class MapBenchmarkConfig extends BenchmarkConfig {
  private static final int DEFAULT_CONCURRENCY = 1;
  private static final int DEFAULT_WRITE_PERCENTAGE = 100;
  private static final int DEFAULT_NUM_KEYS = 100000;
  private static final int DEFAULT_KEY_LENGTH = 32;
  private static final int DEFAULT_NUM_UNIQUE_VALUES = 100;
  private static final int DEFAULT_VALUE_LENGTH = 1024;
  private static final boolean DEFAULT_INCLUDE_EVENTS = false;
  private static final boolean DEFAULT_DETERMINISTIC = true;

  private PrimitiveProtocolConfig protocol;
  private int concurrency = DEFAULT_CONCURRENCY;
  private int writePercentage = DEFAULT_WRITE_PERCENTAGE;
  private int numKeys = DEFAULT_NUM_KEYS;
  private int keyLength = DEFAULT_KEY_LENGTH;
  private int numValues = DEFAULT_NUM_UNIQUE_VALUES;
  private int valueLength = DEFAULT_VALUE_LENGTH;
  private boolean includeEvents = DEFAULT_INCLUDE_EVENTS;
  private boolean deterministic = DEFAULT_DETERMINISTIC;

  public MapBenchmarkConfig() {
  }

  public MapBenchmarkConfig(MapBenchmarkConfig config) {
    super(config);
    this.protocol = config.protocol;
    this.concurrency = config.concurrency;
    this.writePercentage = config.writePercentage;
    this.numKeys = config.numKeys;
    this.keyLength = config.keyLength;
    this.numValues = config.numValues;
    this.valueLength = config.valueLength;
    this.includeEvents = config.includeEvents;
    this.deterministic = config.deterministic;
  }

  @Override
  public BenchmarkConfig copy() {
    return new MapBenchmarkConfig(this);
  }

  @Override
  public BenchmarkType getType() {
    return MapBenchmarkType.INSTANCE;
  }

  @Override
  public MapBenchmarkConfig setBenchId(String benchId) {
    super.setBenchId(benchId);
    return this;
  }

  @Override
  public MapBenchmarkConfig setOperations(int operations) {
    super.setOperations(operations);
    return this;
  }

  public PrimitiveProtocolConfig<?> getProtocol() {
    return protocol;
  }

  public MapBenchmarkConfig setProtocol(PrimitiveProtocolConfig<?> protocol) {
    this.protocol = protocol;
    return this;
  }

  public int getConcurrency() {
    return concurrency;
  }

  public MapBenchmarkConfig setConcurrency(int concurrency) {
    this.concurrency = concurrency;
    return this;
  }

  public int getWritePercentage() {
    return writePercentage;
  }

  public MapBenchmarkConfig setWritePercentage(int writePercentage) {
    this.writePercentage = writePercentage;
    return this;
  }

  public int getNumKeys() {
    return numKeys;
  }

  public MapBenchmarkConfig setNumKeys(int numKeys) {
    this.numKeys = numKeys;
    return this;
  }

  public int getKeyLength() {
    return keyLength;
  }

  public MapBenchmarkConfig setKeyLength(int keyLength) {
    this.keyLength = keyLength;
    return this;
  }

  public int getNumValues() {
    return numValues;
  }

  public MapBenchmarkConfig setNumValues(int numValues) {
    this.numValues = numValues;
    return this;
  }

  public int getValueLength() {
    return valueLength;
  }

  public MapBenchmarkConfig setValueLength(int valueLength) {
    this.valueLength = valueLength;
    return this;
  }

  public boolean isIncludeEvents() {
    return includeEvents;
  }

  public MapBenchmarkConfig setIncludeEvents(boolean includeEvents) {
    this.includeEvents = includeEvents;
    return this;
  }

  public boolean isDeterministic() {
    return deterministic;
  }

  public MapBenchmarkConfig setDeterministic(boolean deterministic) {
    this.deterministic = deterministic;
    return this;
  }
}
