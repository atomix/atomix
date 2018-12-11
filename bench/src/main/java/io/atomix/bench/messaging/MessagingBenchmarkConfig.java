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
package io.atomix.bench.messaging;

import io.atomix.bench.BenchmarkConfig;
import io.atomix.bench.BenchmarkType;

/**
 * Messaging benchmark configuration.
 */
public class MessagingBenchmarkConfig extends BenchmarkConfig {
  private static final int DEFAULT_CONCURRENCY = 1;
  private static final int DEFAULT_MESSAGE_SIZE = 1024;
  private static final Integer DEFAULT_WINDOW_SIZE = null;

  private int concurrency = DEFAULT_CONCURRENCY;
  private int messageSize = DEFAULT_MESSAGE_SIZE;
  private Integer windowSize = DEFAULT_WINDOW_SIZE;

  public MessagingBenchmarkConfig() {
  }

  public MessagingBenchmarkConfig(MessagingBenchmarkConfig config) {
    super(config);
    this.concurrency = config.concurrency;
    this.messageSize = config.messageSize;
  }

  @Override
  public BenchmarkType getType() {
    return MessagingBenchmarkType.INSTANCE;
  }

  @Override
  public BenchmarkConfig copy() {
    return new MessagingBenchmarkConfig(this);
  }

  public int getConcurrency() {
    return concurrency;
  }

  public MessagingBenchmarkConfig setConcurrency(int concurrency) {
    this.concurrency = concurrency;
    return this;
  }

  public int getMessageSize() {
    return messageSize;
  }

  public MessagingBenchmarkConfig setMessageSize(int messageSize) {
    this.messageSize = messageSize;
    return this;
  }

  public Integer getWindowSize() {
    return windowSize;
  }

  public void setWindowSize(Integer windowSize) {
    this.windowSize = windowSize;
  }
}
