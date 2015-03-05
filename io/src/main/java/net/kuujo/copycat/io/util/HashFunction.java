/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.io.util;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.Bytes;
import net.kuujo.copycat.util.internal.Buffers;

import java.nio.ByteBuffer;

/**
 * Hash function.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface HashFunction {

  /**
   * Converts a 64-bit hash value into a 32-bit hash.
   *
   * @param hash The hash value to convert.
   * @return The converted hash value.
   */
  static int asInt(long hash) {
    return (int) (hash - (hash >> 32)) & (2^32 - 1);
  }

  /**
   * Creates a 32-bit hash of the given bytes.
   *
   * @param bytes The bytes to hash.
   * @return The hash value.
   */
  default int asInt(byte[] bytes) {
    long hash = hashBytes(bytes);
    return asInt(hash);
  }

  /**
   * Hashes the remaining bytes in the given buffer.
   *
   * @param buffer The buffer to hash.
   * @return The hashed buffer bytes.
   */
  default long hashBytes(ByteBuffer buffer) {
    return hashBytes(Buffers.getBytes(buffer));
  }

  /**
   * Hashes the remaining bytes in the given buffer.
   *
   * @param buffer The buffer to hash.
   * @return The hashed buffer bytes.
   */
  default long hashBytes(Buffer buffer) {
    byte[] bytes = new byte[(int) buffer.remaining()];
    buffer.read(bytes);
    return hashBytes(bytes);
  }

  /**
   * Hashes the given bytes.
   *
   * @param bytes The bytes to hash.
   * @return The hashed bytes.
   */
  default long hashBytes(Bytes bytes) {
    byte[] barray = new byte[(int) bytes.size()];
    bytes.read(barray, 0, bytes.size());
    return hashBytes(barray);
  }

  /**
   * Hashes the given bytes.
   *
   * @param bytes The bytes to hash.
   * @return The hashed bytes.
   */
  long hashBytes(byte[] bytes);

}
