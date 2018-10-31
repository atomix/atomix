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

import io.atomix.primitive.log.Record;
import io.atomix.utils.misc.ArraySizeHashPrinter;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Read response.
 */
public class ReadResponse extends LogResponse {

  public static ReadResponse ok(List<Record> records) {
    return new ReadResponse(Status.OK, checkNotNull(records));
  }

  public static ReadResponse error() {
    return new ReadResponse(Status.ERROR, null);
  }

  private final List<Record> records;

  private ReadResponse(Status status, List<Record> records) {
    super(status);
    this.records = records;
  }

  public List<Record> records() {
    return records;
  }

  @Override
  public String toString() {
    if (status() == Status.OK) {
      return toStringHelper(this)
          .add("status", status())
          .add("entries", records().stream().map(entry -> ArraySizeHashPrinter.of(entry.value())))
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status())
          .toString();
    }
  }
}
