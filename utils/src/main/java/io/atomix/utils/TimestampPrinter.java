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
package io.atomix.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Timestamp printer.
 */
public class TimestampPrinter {

  /**
   * Returns a new timestamp printer.
   *
   * @param timestamp the timestamp to print
   * @return the timestamp printer
   */
  public static TimestampPrinter of(long timestamp) {
    return new TimestampPrinter(timestamp);
  }

  private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");

  private final long timestamp;

  public TimestampPrinter(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return FORMAT.format(new Date(timestamp));
  }
}
