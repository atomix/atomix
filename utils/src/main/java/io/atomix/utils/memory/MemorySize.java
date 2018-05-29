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
package io.atomix.utils.memory;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Memory size.
 */
public class MemorySize {

  /**
   * Creates a memory size from the given bytes.
   *
   * @param bytes the number of bytes
   * @return the memory size
   */
  public static MemorySize from(long bytes) {
    return new MemorySize(bytes);
  }

  private final long bytes;

  public MemorySize(long bytes) {
    this.bytes = bytes;
  }

  /**
   * Returns the number of bytes.
   *
   * @return the number of bytes
   */
  public long bytes() {
    return bytes;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(bytes).hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof MemorySize && ((MemorySize) object).bytes == bytes;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .addValue(bytes)
        .toString();
  }
}
