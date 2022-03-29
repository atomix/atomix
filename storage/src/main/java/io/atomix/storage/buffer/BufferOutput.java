// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.buffer;

import java.nio.charset.Charset;
import java.util.function.Function;

/**
 * Writable buffer.
 * <p>
 * This interface exposes methods for writing to a byte buffer. Writable buffers maintain a small amount of state
 * regarding current cursor positions and limits similar to the behavior of {@link java.nio.ByteBuffer}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface BufferOutput<T extends BufferOutput<?>> extends AutoCloseable {

  /**
   * Writes an array of bytes to the buffer.
   *
   * @param bytes The array of bytes to write.
   * @return The written buffer.
   */
  T write(Bytes bytes);

  /**
   * Writes an array of bytes to the buffer.
   *
   * @param bytes The array of bytes to write.
   * @return The written buffer.
   */
  T write(byte[] bytes);

  /**
   * Writes an array of bytes to the buffer.
   *
   * @param bytes  The array of bytes to write.
   * @param offset The offset at which to start writing the bytes.
   * @param length The number of bytes from the provided byte array to write to the buffer.
   * @return The written buffer.
   */
  T write(Bytes bytes, int offset, int length);

  /**
   * Writes an array of bytes to the buffer.
   *
   * @param bytes  The array of bytes to write.
   * @param offset The offset at which to start writing the bytes.
   * @param length The number of bytes from the provided byte array to write to the buffer.
   * @return The written buffer.
   */
  T write(byte[] bytes, int offset, int length);

  /**
   * Writes a buffer to the buffer.
   *
   * @param buffer The buffer to write.
   * @return The written buffer.
   */
  T write(Buffer buffer);

  /**
   * Writes an object to the snapshot.
   *
   * @param object the object to write
   * @param encoder the object encoder
   * @return The snapshot writer.
   */
  @SuppressWarnings("unchecked")
  default <U> T writeObject(U object, Function<U, byte[]> encoder) {
    byte[] bytes = encoder.apply(object);
    writeInt(bytes.length).write(bytes);
    return (T) this;
  }

  /**
   * Writes a byte array.
   *
   * @param bytes The byte array to write.
   * @return The written buffer.
   */
  @SuppressWarnings("unchecked")
  default T writeBytes(byte[] bytes) {
    write(bytes);
    return (T) this;
  }

  /**
   * Writes a byte to the buffer.
   *
   * @param b The byte to write.
   * @return The written buffer.
   */
  T writeByte(int b);

  /**
   * Writes an unsigned byte to the buffer.
   *
   * @param b The byte to write.
   * @return The written buffer.
   */
  T writeUnsignedByte(int b);

  /**
   * Writes a 16-bit character to the buffer.
   *
   * @param c The character to write.
   * @return The written buffer.
   */
  T writeChar(char c);

  /**
   * Writes a 16-bit signed integer to the buffer.
   *
   * @param s The short to write.
   * @return The written buffer.
   */
  T writeShort(short s);

  /**
   * Writes a 16-bit unsigned integer to the buffer.
   *
   * @param s The short to write.
   * @return The written buffer.
   */
  T writeUnsignedShort(int s);

  /**
   * Writes a 24-bit signed integer to the buffer.
   *
   * @param m The integer to write.
   * @return The written buffer.
   */
  T writeMedium(int m);

  /**
   * Writes a 24-bit unsigned integer to the buffer.
   *
   * @param m The integer to write.
   * @return The written buffer.
   */
  T writeUnsignedMedium(int m);

  /**
   * Writes a 32-bit signed integer to the buffer.
   *
   * @param i The integer to write.
   * @return The written buffer.
   */
  T writeInt(int i);

  /**
   * Writes a 32-bit unsigned integer to the buffer.
   *
   * @param i The integer to write.
   * @return The written buffer.
   */
  T writeUnsignedInt(long i);

  /**
   * Writes a 64-bit signed integer to the buffer.
   *
   * @param l The long to write.
   * @return The written buffer.
   */
  T writeLong(long l);

  /**
   * Writes a single-precision 32-bit floating point number to the buffer.
   *
   * @param f The float to write.
   * @return The written buffer.
   */
  T writeFloat(float f);

  /**
   * Writes a double-precision 64-bit floating point number to the buffer.
   *
   * @param d The double to write.
   * @return The written buffer.
   */
  T writeDouble(double d);

  /**
   * Writes a 1 byte boolean to the buffer.
   *
   * @param b The boolean to write.
   * @return The written buffer.
   */
  T writeBoolean(boolean b);

  /**
   * Writes a string to the buffer.
   *
   * @param s The string to write.
   * @return The written buffer.
   */
  T writeString(String s);

  /**
   * Writes a string to the buffer.
   *
   * @param s       The string to write.
   * @param charset The character set with which to encode the string.
   * @return The written buffer.
   */
  T writeString(String s, Charset charset);

  /**
   * Writes a UTF-8 string to the buffer.
   *
   * @param s The string to write.
   * @return The written buffer.
   */
  T writeUTF8(String s);

  /**
   * Flushes the buffer to the underlying persistence layer.
   *
   * @return The flushed buffer.
   */
  T flush();

  @Override
  void close();

}
