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
 * Navigable byte buffer for input/output operations.
 * <p>
 * The byte buffer provides a fluent interface for reading bytes from and writing bytes to some underlying storage
 * implementation. The {@code Buffer} type is agnostic about the specific underlying storage implementation, but different
 * buffer implementations may be designed for specific storage layers.
 * <p>
 * Aside from the underlying storage implementation, this buffer works very similarly to Java's
 * {@link java.nio.ByteBuffer}. It intentionally exposes methods that can be easily understood by any developer with
 * experience with {@code ByteBuffer}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Buffer extends Bytes<Buffer>, AutoCloseable {

  /**
   * Reads bytes into the given byte array.
   * <p>
   * Bytes will be read starting at the current buffer position until either the byte array {@code length} or the
   * {@link net.kuujo.copycat.io.Buffer#limit()} has been reached. If {@link net.kuujo.copycat.io.Buffer#remaining()}
   * is less than the {@code length} of the given byte array, a {@link java.nio.BufferUnderflowException} will be
   * thrown.
   *
   * @param bytes The byte array into which to read bytes.
   * @return The buffer.
   * @throws java.nio.BufferUnderflowException
   */
  @Override
  Buffer read(byte[] bytes);

  /**
   * Reads bytes into the given byte array starting at the given offset up to the given length.
   * <p>
   * Bytes will be read from the given starting offset up to the given length. If the provided {@code length} is
   * greater than {@link net.kuujo.copycat.io.Buffer#remaining()} then a {@link java.nio.BufferUnderflowException} will
   * be thrown. If the {@code offset} is out of bounds of the buffer then an {@link java.lang.IndexOutOfBoundsException}
   * will be thrown.
   *
   * @param bytes The byte array into which to read bytes.
   * @param offset The offset from which to start reading bytes.
   * @param length The total number of bytes to read.
   * @return The buffer.
   * @throws java.nio.BufferUnderflowException If {@code length} is greater than {@link net.kuujo.copycat.io.Buffer#remaining()}
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  Buffer read(byte[] bytes, long offset, int length);

  /**
   * Reads a byte from the buffer at the current position.
   * <p>
   * When the byte is read from the buffer, the buffer's {@code position} will be advanced by {@link Byte#BYTES}. If
   * there are no bytes remaining in the buffer then a {@link java.nio.BufferUnderflowException} will be thrown.
   *
   * @return The read byte.
   * @throws java.nio.BufferUnderflowException If {@link net.kuujo.copycat.io.Buffer#remaining()} is less than {@link Byte#BYTES}
   */
  @Override
  int readByte();

  /**
   * Reads a byte from the buffer at the given offset.
   * <p>
   * The byte will be read from the given offset. If the given index is out of the bounds of the buffer then a
   * {@link java.lang.IndexOutOfBoundsException} will be thrown.
   *
   * @param offset The offset at which to read the byte.
   * @return The read byte.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  int readByte(long offset);

  /**
   * Reads a 16-bit character from the buffer at the current position.
   * <p>
   * When the character is read from the buffer, the buffer's {@code position} will be advanced by {@link Character#BYTES}.
   * If there are less than {@link Character#BYTES} bytes remaining in the buffer then a
   * {@link java.nio.BufferUnderflowException} will be thrown.
   *
   * @return The read character.
   * @throws java.nio.BufferUnderflowException If {@link net.kuujo.copycat.io.Buffer#remaining()} is less than {@link Character#BYTES}
   */
  @Override
  char readChar();

  /**
   * Reads a 16-bit character from the buffer at the given offset.
   * <p>
   * The character will be read from the given offset. If the given index is out of the bounds of the buffer then a
   * {@link java.lang.IndexOutOfBoundsException} will be thrown.
   *
   * @param offset The offset at which to read the character.
   * @return The read character.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  char readChar(long offset);

  /**
   * Reads a 16-bit signed integer from the buffer at the current position.
   * <p>
   * When the short is read from the buffer, the buffer's {@code position} will be advanced by {@link Short#BYTES}.
   * If there are less than {@link Short#BYTES} bytes remaining in the buffer then a
   * {@link java.nio.BufferUnderflowException} will be thrown.
   *
   * @return The read short.
   * @throws java.nio.BufferUnderflowException If {@link net.kuujo.copycat.io.Buffer#remaining()} is less than {@link Short#BYTES}
   */
  @Override
  short readShort();

  /**
   * Reads a 16-bit signed integer from the buffer at the given offset.
   * <p>
   * The short will be read from the given offset. If the given index is out of the bounds of the buffer then a
   * {@link java.lang.IndexOutOfBoundsException} will be thrown.
   *
   * @param offset The offset at which to read the short.
   * @return The read short.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  short readShort(long offset);

  /**
   * Reads a 32-bit signed integer from the buffer at the current position.
   * <p>
   * When the integer is read from the buffer, the buffer's {@code position} will be advanced by {@link Integer#BYTES}.
   * If there are less than {@link Integer#BYTES} bytes remaining in the buffer then a
   * {@link java.nio.BufferUnderflowException} will be thrown.
   *
   * @return The read integer.
   * @throws java.nio.BufferUnderflowException If {@link net.kuujo.copycat.io.Buffer#remaining()} is less than {@link Integer#BYTES}
   */
  @Override
  int readInt();

  /**
   * Reads a 32-bit signed integer from the buffer at the given offset.
   * <p>
   * The integer will be read from the given offset. If the given index is out of the bounds of the buffer then a
   * {@link java.lang.IndexOutOfBoundsException} will be thrown.
   *
   * @param offset The offset at which to read the integer.
   * @return The read integer.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  int readInt(long offset);

  /**
   * Reads a 64-bit signed integer from the buffer at the current position.
   * <p>
   * When the long is read from the buffer, the buffer's {@code position} will be advanced by {@link Long#BYTES}.
   * If there are less than {@link Long#BYTES} bytes remaining in the buffer then a
   * {@link java.nio.BufferUnderflowException} will be thrown.
   *
   * @return The read long.
   * @throws java.nio.BufferUnderflowException If {@link net.kuujo.copycat.io.Buffer#remaining()} is less than {@link Long#BYTES}
   */
  @Override
  long readLong();

  /**
   * Reads a 64-bit signed integer from the buffer at the given offset.
   * <p>
   * The long will be read from the given offset. If the given index is out of the bounds of the buffer then a
   * {@link java.lang.IndexOutOfBoundsException} will be thrown.
   *
   * @param offset The offset at which to read the long.
   * @return The read long.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  long readLong(long offset);

  /**
   * Reads a single-precision 32-bit floating point number from the buffer at the current position.
   * <p>
   * When the float is read from the buffer, the buffer's {@code position} will be advanced by {@link Float#BYTES}.
   * If there are less than {@link Float#BYTES} bytes remaining in the buffer then a
   * {@link java.nio.BufferUnderflowException} will be thrown.
   *
   * @return The read float.
   * @throws java.nio.BufferUnderflowException If {@link net.kuujo.copycat.io.Buffer#remaining()} is less than {@link Float#BYTES}
   */
  @Override
  float readFloat();

  /**
   * Reads a single-precision 32-bit floating point number from the buffer at the given offset.
   * <p>
   * The float will be read from the given offset. If the given index is out of the bounds of the buffer then a
   * {@link java.lang.IndexOutOfBoundsException} will be thrown.
   *
   * @param offset The offset at which to read the float.
   * @return The read float.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  float readFloat(long offset);

  /**
   * Reads a double-precision 64-bit floating point number from the buffer at the current position.
   * <p>
   * When the double is read from the buffer, the buffer's {@code position} will be advanced by {@link Double#BYTES}.
   * If there are less than {@link Double#BYTES} bytes remaining in the buffer then a
   * {@link java.nio.BufferUnderflowException} will be thrown.
   *
   * @return The read double.
   * @throws java.nio.BufferUnderflowException If {@link net.kuujo.copycat.io.Buffer#remaining()} is less than {@link Double#BYTES}
   */
  @Override
  double readDouble();

  /**
   * Reads a double-precision 64-bit floating point number from the buffer at the given offset.
   * <p>
   * The double will be read from the given offset. If the given index is out of the bounds of the buffer then a
   * {@link java.lang.IndexOutOfBoundsException} will be thrown.
   *
   * @param offset The offset at which to read the double.
   * @return The read double.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  double readDouble(long offset);

  /**
   * Reads a 1 byte boolean from the buffer at the current position.
   * <p>
   * When the short is read from the buffer, the buffer's {@code position} will be advanced by {@code 1}.
   * If there are no bytes remaining in the buffer then a {@link java.nio.BufferUnderflowException} will be thrown.
   *
   * @return The read boolean.
   * @throws java.nio.BufferUnderflowException If {@link net.kuujo.copycat.io.Buffer#remaining()} is less than {@code 1}
   */
  @Override
  boolean readBoolean();

  /**
   * Reads a 1 byte boolean from the buffer at the given offset.
   * <p>
   * The boolean will be read from the given offset. If the given index is out of the bounds of the buffer then a
   * {@link java.lang.IndexOutOfBoundsException} will be thrown.
   *
   * @param offset The offset at which to read the boolean.
   * @return The read boolean.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  boolean readBoolean(long offset);

  /**
   * Writes an array of bytes to the buffer.
   * <p>
   * When the bytes are written to the buffer, the buffer's {@code position} will be advanced by the number of bytes
   * in the provided byte array. If the number of bytes exceeds {@link Buffer#limit()} then an
   * {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param bytes The array of bytes to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If the number of bytes exceeds the buffer's remaining bytes.
   */
  @Override
  Buffer write(byte[] bytes);

  /**
   * Writes an array of bytes to the buffer.
   * <p>
   * The bytes will be written starting at the given offset up to the given length. If the length of the byte array
   * is larger than the provided {@code length} then only {@code length} bytes will be read from the array. If the
   * provided {@code length} is greater than the remaining bytes in this buffer then a {@link java.nio.BufferOverflowException}
   * will be thrown.
   *
   * @param bytes The array of bytes to write.
   * @param offset The offset at which to start writing the bytes.
   * @param length The number of bytes from the provided byte array to write to the buffer.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If there are not enough bytes remaining in the buffer.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer.
   */
  @Override
  Buffer write(byte[] bytes, long offset, int length);

  /**
   * Writes a byte to the buffer at the current position.
   * <p>
   * When the byte is written to the buffer, the buffer's {@code position} will be advanced by {@link Byte#BYTES}. If
   * there are no bytes remaining in the buffer then a {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param b The byte to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If there are no bytes remaining in the buffer.
   */
  @Override
  Buffer writeByte(int b);

  /**
   * Writes a byte to the buffer at the given offset.
   * <p>
   * The byte will be written at the given offset. If there are no bytes remaining in the buffer then a
   * {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param offset The offset at which to write the byte.
   * @param b The byte to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If there are not enough bytes remaining in the buffer.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  Buffer writeByte(long offset, int b);

  /**
   * Writes a 16-bit character to the buffer at the current position.
   * <p>
   * When the character is written to the buffer, the buffer's {@code position} will be advanced by
   * {@link Character#BYTES}. If less than {@code 2} bytes are remaining in the buffer then a
   * {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param c The character to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Character#BYTES}.
   */
  @Override
  Buffer writeChar(char c);

  /**
   * Writes a 16-bit character to the buffer at the given offset.
   * <p>
   * The character will be written at the given offset. If there are less than {@link Character#BYTES} bytes remaining
   * in the buffer then a {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param offset The offset at which to write the character.
   * @param c The character to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Character#BYTES}.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  Buffer writeChar(long offset, char c);

  /**
   * Writes a 16-bit signed integer to the buffer at the current position.
   * <p>
   * When the short is written to the buffer, the buffer's {@code position} will be advanced by {@link Short#BYTES}. If
   * less than {@link Short#BYTES} bytes are remaining in the buffer then a {@link java.nio.BufferOverflowException}
   * will be thrown.
   *
   * @param s The short to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Short#BYTES}.
   */
  @Override
  Buffer writeShort(short s);

  /**
   * Writes a 16-bit signed integer to the buffer at the given offset.
   * <p>
   * The short will be written at the given offset. If there are less than {@link Short#BYTES} bytes remaining in the buffer
   * then a {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param offset The offset at which to write the short.
   * @param s The short to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Short#BYTES}.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  Buffer writeShort(long offset, short s);

  /**
   * Writes a 32-bit signed integer to the buffer at the current position.
   * <p>
   * When the integer is written to the buffer, the buffer's {@code position} will be advanced by {@link Integer#BYTES}.
   * If less than {@link Integer#BYTES} bytes are remaining in the buffer then a {@link java.nio.BufferOverflowException}
   * will be thrown.
   *
   * @param i The integer to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Integer#BYTES}.
   */
  @Override
  Buffer writeInt(int i);

  /**
   * Writes a 32-bit signed integer to the buffer at the given offset.
   * <p>
   * The integer will be written at the given offset. If there are less than {@link Integer#BYTES} bytes remaining
   * in the buffer then a {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param offset The offset at which to write the integer.
   * @param i The integer to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Integer#BYTES}.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  Buffer writeInt(long offset, int i);

  /**
   * Writes a 64-bit signed integer to the buffer at the current position.
   * <p>
   * When the long is written to the buffer, the buffer's {@code position} will be advanced by {@link Long#BYTES}.
   * If less than {@link Long#BYTES} bytes are remaining in the buffer then a {@link java.nio.BufferOverflowException}
   * will be thrown.
   *
   * @param l The long to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Long#BYTES}.
   */
  @Override
  Buffer writeLong(long l);

  /**
   * Writes a 64-bit signed integer to the buffer at the given offset.
   * <p>
   * The long will be written at the given offset. If there are less than {@link Long#BYTES} bytes remaining
   * in the buffer then a {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param offset The offset at which to write the long.
   * @param l The long to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Long#BYTES}.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  Buffer writeLong(long offset, long l);

  /**
   * Writes a single-precision 32-bit floating point number to the buffer at the current position.
   * <p>
   * When the float is written to the buffer, the buffer's {@code position} will be advanced by {@link Float#BYTES}.
   * If less than {@link Float#BYTES} bytes are remaining in the buffer then a {@link java.nio.BufferOverflowException}
   * will be thrown.
   *
   * @param f The float to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Float#BYTES}.
   */
  @Override
  Buffer writeFloat(float f);

  /**
   * Writes a single-precision 32-bit floating point number to the buffer at the given offset.
   * <p>
   * The float will be written at the given offset. If there are less than {@link Float#BYTES} bytes remaining
   * in the buffer then a {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param offset The offset at which to write the float.
   * @param f The float to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Float#BYTES}.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  Buffer writeFloat(long offset, float f);

  /**
   * Writes a double-precision 64-bit floating point number to the buffer at the current position.
   * <p>
   * When the double is written to the buffer, the buffer's {@code position} will be advanced by {@link Double#BYTES}.
   * If less than {@link Double#BYTES} bytes are remaining in the buffer then a {@link java.nio.BufferOverflowException}
   * will be thrown.
   *
   * @param d The double to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Double#BYTES}.
   */
  @Override
  Buffer writeDouble(double d);

  /**
   * Writes a double-precision 64-bit floating point number to the buffer at the given offset.
   * <p>
   * The double will be written at the given offset. If there are less than {@link Double#BYTES} bytes remaining
   * in the buffer then a {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param offset The offset at which to write the double.
   * @param d The double to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@link Double#BYTES}.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  Buffer writeDouble(long offset, double d);

  /**
   * Writes a 1 byte boolean to the buffer at the current position.
   * <p>
   * When the boolean is written to the buffer, the buffer's {@code position} will be advanced by {@code 1}.
   * If there are no bytes remaining in the buffer then a {@link java.nio.BufferOverflowException}
   * will be thrown.
   *
   * @param b The boolean to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If the number of bytes exceeds the buffer's remaining bytes.
   */
  @Override
  Buffer writeBoolean(boolean b);

  /**
   * Writes a 1 byte boolean to the buffer at the given offset.
   * <p>
   * The boolean will be written as a single byte at the given offset. If there are no bytes remaining in the buffer
   * then a {@link java.nio.BufferOverflowException} will be thrown.
   *
   * @param offset The offset at which to write the boolean.
   * @param b The boolean to write.
   * @return The written buffer.
   * @throws java.nio.BufferOverflowException If {@link Buffer#remaining()} is less than {@code 1}.
   * @throws java.lang.IndexOutOfBoundsException If the given offset is out of the bounds of the buffer. Note that
   *         bounds are determined by the buffer's {@link net.kuujo.copycat.io.Buffer#limit()} rather than capacity.
   */
  @Override
  Buffer writeBoolean(long offset, boolean b);

}
