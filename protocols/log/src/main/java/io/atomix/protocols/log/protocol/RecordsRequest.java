/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.log.protocol;

import io.atomix.primitive.log.LogRecord;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Records request.
 */
public class RecordsRequest extends LogRequest {

  public static RecordsRequest request(LogRecord record, boolean reset) {
    return new RecordsRequest(record, reset);
  }

  private final LogRecord record;
  private final boolean reset;

  private RecordsRequest(LogRecord record, boolean reset) {
    this.record = record;
    this.reset = reset;
  }

  public LogRecord record() {
    return record;
  }

  public boolean reset() {
    return reset;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("record", record)
        .add("reset", reset)
        .toString();
  }
}
