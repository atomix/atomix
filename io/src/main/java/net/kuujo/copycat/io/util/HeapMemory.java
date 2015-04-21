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

import sun.misc.Unsafe;

/**
 * Java heap memory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HeapMemory implements Memory {
  public static final long ARRAY_BASE_OFFSET = NativeMemory.UNSAFE.arrayBaseOffset(byte[].class);
  public static final long MAX_SIZE = Integer.MAX_VALUE - 5;

  /**
   * Allocates heap memory via {@link HeapMemoryAllocator}.
   *
   * @param size The size of the memory to allocate.
   * @return The allocated memory.
   * @throws IllegalArgumentException If {@code size} is greater than the maximum allowed size for
   *         an array on the Java heap - {@code Integer.MAX_VALUE - 5}
   */
  public static HeapMemory allocate(long size) {
    if (size > MAX_SIZE)
      throw new IllegalArgumentException("size cannot be greater than " + MAX_SIZE);
    return new HeapMemoryAllocator().allocate(size);
  }

  private final HeapMemoryAllocator allocator;
  private final byte[] array;

  public HeapMemory(byte[] array, HeapMemoryAllocator allocator) {
    if (array == null)
      throw new NullPointerException("array cannot be null");
    if (allocator == null)
      throw new NullPointerException("allocator cannot be null");
    this.allocator = allocator;
    this.array = array;
  }

  @Override
  public HeapMemoryAllocator allocator() {
    return allocator;
  }

  @Override
  public final long address() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final long address(long offset) {
    return ARRAY_BASE_OFFSET + offset;
  }

  @Override
  public long size() {
    return array.length;
  }

  /**
   * Returns the native Unsafe memory object.
   *
   * @return The native Unsafe memory object.
   */
  public Unsafe unsafe() {
    return NativeMemory.UNSAFE;
  }

  /**
   * Returns the underlying byte array.
   *
   * @return The underlying byte array.
   */
  public final byte[] array() {
    return array;
  }

  /**
   * Returns the array base offset.
   *
   * @return The array base offset.
   */
  public final long offset() {
    return ARRAY_BASE_OFFSET;
  }

  @Override
  public HeapMemory copy() {
    HeapMemory copy = allocator.allocate(array.length);
    NativeMemory.UNSAFE.copyMemory(array, ARRAY_BASE_OFFSET, copy.array, ARRAY_BASE_OFFSET, array.length);
    return copy;
  }

  @Override
  public byte getByte(long offset) {
    return NativeMemory.UNSAFE.getByte(array, address(offset));
  }

  @Override
  public char getChar(long offset) {
    return NativeMemory.UNSAFE.getChar(array, address(offset));
  }

  @Override
  public short getShort(long offset) {
    return NativeMemory.UNSAFE.getShort(array, address(offset));
  }

  @Override
  public int getInt(long offset) {
    return NativeMemory.UNSAFE.getInt(array, address(offset));
  }

  @Override
  public long getLong(long offset) {
    return NativeMemory.UNSAFE.getLong(array, address(offset));
  }

  @Override
  public float getFloat(long offset) {
    return NativeMemory.UNSAFE.getFloat(array, address(offset));
  }

  @Override
  public double getDouble(long offset) {
    return NativeMemory.UNSAFE.getDouble(array, address(offset));
  }

  @Override
  public void putByte(long offset, byte b) {
    NativeMemory.UNSAFE.putByte(array, address(offset), b);
  }

  @Override
  public void putChar(long offset, char c) {
    NativeMemory.UNSAFE.putChar(array, address(offset), c);
  }

  @Override
  public void putShort(long offset, short s) {
    NativeMemory.UNSAFE.putShort(array, address(offset), s);
  }

  @Override
  public void putInt(long offset, int i) {
    NativeMemory.UNSAFE.putInt(array, address(offset), i);
  }

  @Override
  public void putLong(long offset, long l) {
    NativeMemory.UNSAFE.putLong(array, address(offset), l);
  }

  @Override
  public void putFloat(long offset, float f) {
    NativeMemory.UNSAFE.putFloat(array, address(offset), f);
  }

  @Override
  public void putDouble(long offset, double d) {
    NativeMemory.UNSAFE.putDouble(array, address(offset), d);
  }

  @Override
  public void clear() {
    NativeMemory.UNSAFE.setMemory(array, ARRAY_BASE_OFFSET, array.length, (byte) 0);
  }

  @Override
  public void free() {
    clear();
  }

}
