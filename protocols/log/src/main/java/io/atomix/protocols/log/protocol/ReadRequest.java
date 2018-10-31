/*
 * Copyright 2018-present Open Networking Foundation
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

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Read request.
 */
public class ReadRequest extends LogRequest {

  public static ReadRequest request(long index, int batchSize) {
    return new ReadRequest(index, batchSize);
  }

  private final long index;
  private final int batchSize;

  private ReadRequest(long index, int batchSize) {
    this.index = index;
    this.batchSize = batchSize;
  }

  public long index() {
    return index;
  }

  public int batchSize() {
    return batchSize;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index())
        .add("batchSize", batchSize())
        .toString();
  }
}
