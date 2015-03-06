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

/**
 * Writable bytes.
 * <p>
 * This interface exposes methods for writing bytes to specific positions in a byte array.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface BytesOutput<T extends BytesOutput<T>> {

  /**
   * Writes an array of bytes to the buffer.
   *
   * @param bytes The array of bytes to write.
   * @param offset The offset at which to start writing the bytes.
   * @param length The number of bytes from the provided byte array to write to the buffer.
   * @return The written buffer.
   */
  T write(Bytes bytes, long offset, long length);

  /**
   * Writes an array of bytes to the buffer.
   *
   * @param bytes The array of bytes to write.
   * @param offset The offset at which to start writing the bytes.
   * @param length The number of bytes from the provided byte array to write to the buffer.
   * @return The written buffer.
   */
  T write(byte[] bytes, long offset, long length);

  /**
   * Writes a byte to the buffer at the given offset.
   *
   * @param offset The offset at which to write the byte.
   * @param b The byte to write.
   * @return The written buffer.
   */
  T writeByte(long offset, int b);

  /**
   * Writes an unsigned byte to the buffer at the given position.
   *
   * @param offset The offset at which to write the byte.
   * @param b The byte to write.
   * @return The written buffer.
   */
  T writeUnsignedByte(long offset, int b);

  /**
   * Writes a 16-bit character to the buffer at the given offset.
   *
   * @param offset The offset at which to write the character.
   * @param c The character to write.
   * @return The written buffer.
   */
  T writeChar(long offset, char c);

  /**
   * Writes a 16-bit signed integer to the buffer at the given offset.
   *
   * @param offset The offset at which to write the short.
   * @param s The short to write.
   * @return The written buffer.
   */
  T writeShort(long offset, short s);

  /**
   * Writes a 16-bit unsigned integer to the buffer at the given offset.
   *
   * @param offset The offset at which to write the short.
   * @param s The short to write.
   * @return The written buffer.
   */
  T writeUnsignedShort(long offset, int s);

  /**
   * Writes a 32-bit signed integer to the buffer at the given offset.
   *
   * @param offset The offset at which to write the integer.
   * @param i The integer to write.
   * @return The written buffer.
   */
  T writeInt(long offset, int i);

  /**
   * Writes a 32-bit unsigned integer to the buffer at the given offset.
   *
   * @param offset The offset at which to write the integer.
   * @param i The integer to write.
   * @return The written buffer.
   */
  T writeUnsignedInt(long offset, long i);

  /**
   * Writes a 64-bit signed integer to the buffer at the given offset.
   *
   * @param offset The offset at which to write the long.
   * @param l The long to write.
   * @return The written buffer.
   */
  T writeLong(long offset, long l);

  /**
   * Writes a single-precision 32-bit floating point number to the buffer at the given offset.
   *
   * @param offset The offset at which to write the float.
   * @param f The float to write.
   * @return The written buffer.
   */
  T writeFloat(long offset, float f);

  /**
   * Writes a double-precision 64-bit floating point number to the buffer at the given offset.
   *
   * @param offset The offset at which to write the double.
   * @param d The double to write.
   * @return The written buffer.
   */
  T writeDouble(long offset, double d);

  /**
   * Writes a 1 byte boolean to the buffer at the given offset.
   *
   * @param offset The offset at which to write the boolean.
   * @param b The boolean to write.
   * @return The written buffer.
   */
  T writeBoolean(long offset, boolean b);

  /**
   * Flushes the bytes to the underlying persistence layer.
   *
   * @return The flushed buffer.
   */
  T flush();

}
