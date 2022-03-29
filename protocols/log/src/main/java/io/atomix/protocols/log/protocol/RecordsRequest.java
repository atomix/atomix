// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
