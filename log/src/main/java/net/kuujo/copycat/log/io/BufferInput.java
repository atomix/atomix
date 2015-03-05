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
package net.kuujo.copycat.log.io;

/**
 * Readable buffer.
 * <p>
 * This interface exposes methods for reading from a byte buffer. Readable buffers maintain a small amount of state
 * regarding current cursor positions and limits similar to the behavior of {@link java.nio.ByteBuffer}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface BufferInput<T extends BufferInput<?>> {

  /**
   * Reads bytes into the given byte array.
   *
   * @param bytes The byte array into which to read bytes.
   * @return The buffer.
   */
  T read(Bytes bytes);

  /**
   * Reads bytes into the given byte array.
   *
   * @param bytes The byte array into which to read bytes.
   * @return The buffer.
   */
  T read(byte[] bytes);

  /**
   * Reads a byte from the buffer at the current position.
   *
   * @return The read byte.
   */
  int readByte();

  /**
   * Reads a 16-bit character from the buffer at the current position.
   *
   * @return The read character.
   */
  char readChar();

  /**
   * Reads a 16-bit signed integer from the buffer at the current position.
   *
   * @return The read short.
   */
  short readShort();

  /**
   * Reads a 32-bit signed integer from the buffer at the current position.
   *
   * @return The read integer.
   */
  int readInt();

  /**
   * Reads a 64-bit signed integer from the buffer at the current position.
   *
   * @return The read long.
   */
  long readLong();

  /**
   * Reads a single-precision 32-bit floating point number from the buffer at the current position.
   *
   * @return The read float.
   */
  float readFloat();

  /**
   * Reads a double-precision 64-bit floating point number from the buffer at the current position.
   *
   * @return The read double.
   */
  double readDouble();

  /**
   * Reads a 1 byte boolean from the buffer at the current position.
   *
   * @return The read boolean.
   */
  boolean readBoolean();

}
