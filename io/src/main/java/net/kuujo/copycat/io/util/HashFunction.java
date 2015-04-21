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

import java.nio.ByteBuffer;

/**
 * Hash function.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface HashFunction {
  static ThreadLocal<byte[]> BYTES = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[1024];
    }
  };

  /**
   * Converts a 64-bit hash value into a 32-bit hash.
   * <p>
   * The hash is converted from 64-bit to 32-bit using {@code (int) (hash - (hash >> 32)) & (2^32 - 1)}.
   *
   * @param hash The hash value to convert.
   * @return The converted hash value.
   */
  static int asInt(long hash) {
    return (int) (hash - (hash >> 32)) & (2^32 - 1);
  }

  /**
   * Creates a 32-bit hash of the given bytes.
   * <p>
   * The hash is converted from a 64-bit hash via {@link net.kuujo.copycat.io.util.HashFunction#asInt(long)}
   *
   * @param bytes The bytes to hash.
   * @return The hash value.
   */
  default int hash32(byte[] bytes) {
    return asInt(hash64(bytes));
  }

  /**
   * Creates a 32-bit hash of the given buffer.
   * <p>
   * The hash is converted from a 64-bit hash via {@link net.kuujo.copycat.io.util.HashFunction#asInt(long)}
   *
   * @param buffer The buffer to hash.
   * @return The hash value.
   */
  default int hash32(ByteBuffer buffer) {
    return asInt(hash64(buffer));
  }

  /**
   * Creates a 32-bit hash of the given buffer.
   * <p>
   * The hash is converted from a 64-bit hash via {@link net.kuujo.copycat.io.util.HashFunction#asInt(long)}
   *
   * @param buffer The buffer to hash.
   * @return The hash value.
   */
  default int hash32(Buffer buffer) {
    return asInt(hash64(buffer));
  }

  /**
   * Creates a 32-bit hash of the given bytes.
   * <p>
   * The hash is converted from a 64-bit hash via {@link net.kuujo.copycat.io.util.HashFunction#asInt(long)}
   *
   * @param bytes The bytes to hash.
   * @return The hash value.
   */
  default int hash32(Bytes bytes) {
    return asInt(hash64(bytes));
  }

  /**
   * Hashes the remaining bytes in the given buffer.
   *
   * @param buffer The buffer to hash.
   * @return The hashed buffer bytes.
   */
  default long hash64(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return hash64(bytes);
  }

  /**
   * Hashes the remaining bytes in the given buffer.
   *
   * @param buffer The buffer to hash.
   * @return The hashed buffer bytes.
   */
  default long hash64(Buffer buffer) {
    return hash64(buffer.bytes(), buffer.offset() + buffer.position(), buffer.remaining());
  }

  /**
   * Hashes the given bytes.
   *
   * @param buffer The bytes to hash.
   * @return The hashed bytes.
   */
  default long hash64(Bytes buffer) {
    return hash64(buffer, 0, buffer.size());
  }

  /**
   * Hashes the given bytes.
   *
   * @param buffer The bytes to hash.
   * @param offset The offset from which to read bytes.
   * @param length The length of the bytes to read.
   * @return The hash result.
   */
  default long hash64(Bytes buffer, long offset, long length) {
    if (length > Integer.MAX_VALUE)
      throw new IllegalArgumentException("bytes length cannot be greater than " + Integer.MAX_VALUE);
    byte[] bytes = BYTES.get();
    if (bytes.length < length) {
      int bytesLength = bytes.length;
      while (bytesLength < length) {
        bytesLength <<= 1;
      }
      bytes = new byte[bytesLength];
      BYTES.set(bytes);
    }
    buffer.read(offset, bytes, 0, length);
    return hash64(bytes, 0, (int) length);
  }

  /**
   * Hashes the given bytes.
   *
   * @param bytes The bytes to hash.
   * @return The hashed bytes.
   */
  default long hash64(byte[] bytes) {
    return hash64(bytes, 0, bytes.length);
  }

  /**
   * Hashes the given bytes.
   *
   * @param bytes The bytes to hash.
   * @param offset The starting offset in the bytes to read.
   * @param length The length of the bytes to read.
   * @return The hash result.
   */
  long hash64(byte[] bytes, int offset, int length);

}
