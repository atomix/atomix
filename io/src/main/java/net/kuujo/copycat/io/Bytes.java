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
package net.kuujo.copycat.io;

import java.nio.ByteOrder;

/**
 * Common interface for interacting with a memory or disk based array of bytes.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Bytes extends BytesInput<Bytes>, BytesOutput<Bytes>, AutoCloseable {

  /**
   * Returns the size of the bytes.
   *
   * @return The size of the bytes.
   */
  long size();

  /**
   * Resizes the bytes.
   * <p>
   * When the bytes are resized, underlying memory addresses in copies of this instance may no longer be valid. Additionally,
   * if the {@code newSize} is smaller than the current {@code size} then some data may be lost during the resize. Use
   * with caution.
   *
   * @param newSize The size to which to resize this instance.
   * @return The resized bytes.
   */
  Bytes resize(long newSize);

  /**
   * Returns the byte order.
   * <p>
   * For consistency with {@link java.nio.ByteBuffer}, all bytes implementations are initially in {@link java.nio.ByteOrder#BIG_ENDIAN} order.
   *
   * @return The byte order.
   */
  ByteOrder order();

  /**
   * Sets the byte order, returning a new swapped {@link Bytes} instance.
   * <p>
   * By default, all bytes are read and written in {@link java.nio.ByteOrder#BIG_ENDIAN} order. This provides complete
   * consistency with {@link java.nio.ByteBuffer}. To flip bytes to {@link java.nio.ByteOrder#LITTLE_ENDIAN} order, this
   * {@code Bytes} instance is decorated by a {@link net.kuujo.copycat.io.SwappedBytes} instance which will reverse
   * read and written bytes using, e.g. {@link Integer#reverseBytes(int)}.
   *
   * @param order The byte order.
   * @return The updated bytes.
   * @throws NullPointerException If the {@code order} is {@code null}
   */
  Bytes order(ByteOrder order);

  /**
   * Returns a boolean value indicating whether the bytes are direct.
   *
   * @return Indicates whether the bytes are direct.
   */
  default boolean isDirect() {
    return false;
  }

  /**
   * Returns a boolean value indicating whether the bytes are backed by a file.
   *
   * @return Indicates whether the bytes are backed by a file.
   */
  default boolean isFile() {
    return false;
  }

  @Override
  void close();

}
