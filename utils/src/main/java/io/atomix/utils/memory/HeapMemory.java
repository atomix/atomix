/*
 * Copyright 2017-present Open Networking Foundation
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

import sun.misc.Unsafe;

/**
 * Java heap memory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HeapMemory implements Memory {
  public static final int ARRAY_BASE_OFFSET = NativeMemory.UNSAFE.arrayBaseOffset(byte[].class);
  public static final int MAX_SIZE = Integer.MAX_VALUE;

  /**
   * Allocates heap memory via {@link HeapMemoryAllocator}.
   *
   * @param size The count of the memory to allocate.
   * @return The allocated memory.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed count for
   *                                  an array on the Java heap - {@code Integer.MAX_VALUE - 5}
   */
  public static HeapMemory allocate(int size) {
    return new HeapMemoryAllocator().allocate(size);
  }

  /**
   * Wraps the given bytes in a {@link HeapMemory} object.
   *
   * @param bytes The bytes to wrap.
   * @return The wrapped bytes.
   */
  public static HeapMemory wrap(byte[] bytes) {
    return new HeapMemory(bytes, new HeapMemoryAllocator());
  }

  private final HeapMemoryAllocator allocator;
  private byte[] array;

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

  /**
   * Resets the memory pointer.
   *
   * @param array The memory array.
   * @return The heap memory.
   */
  public HeapMemory reset(byte[] array) {
    this.array = array;
    return this;
  }

  @Override
  public final long address(int offset) {
    return ARRAY_BASE_OFFSET + offset;
  }

  @Override
  public int size() {
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
  public final int offset() {
    return ARRAY_BASE_OFFSET;
  }

  @Override
  public HeapMemory copy() {
    HeapMemory copy = allocator.allocate(array.length);
    NativeMemory.UNSAFE.copyMemory(array, ARRAY_BASE_OFFSET, copy.array, ARRAY_BASE_OFFSET, array.length);
    return copy;
  }

  @Override
  public byte getByte(int offset) {
    return NativeMemory.UNSAFE.getByte(array, address(offset));
  }

  @Override
  public char getChar(int offset) {
    if (NativeMemory.BIG_ENDIAN) {
      return NativeMemory.UNSAFE.getChar(array, address(offset));
    } else {
      return Character.reverseBytes(NativeMemory.UNSAFE.getChar(array, address(offset)));
    }
  }

  @Override
  public short getShort(int offset) {
    if (NativeMemory.BIG_ENDIAN) {
      return NativeMemory.UNSAFE.getShort(array, address(offset));
    } else {
      return Short.reverseBytes(NativeMemory.UNSAFE.getShort(array, address(offset)));
    }
  }

  @Override
  public int getInt(int offset) {
    if (NativeMemory.BIG_ENDIAN) {
      return NativeMemory.UNSAFE.getInt(array, address(offset));
    } else {
      return Integer.reverseBytes(NativeMemory.UNSAFE.getInt(array, address(offset)));
    }
  }

  @Override
  public long getLong(int offset) {
    if (NativeMemory.BIG_ENDIAN) {
      return NativeMemory.UNSAFE.getLong(array, address(offset));
    } else {
      return Long.reverseBytes(NativeMemory.UNSAFE.getLong(array, address(offset)));
    }
  }

  @Override
  public float getFloat(int offset) {
    if (NativeMemory.BIG_ENDIAN) {
      return NativeMemory.UNSAFE.getFloat(array, address(offset));
    } else {
      return Float.intBitsToFloat(NativeMemory.UNSAFE.getInt(array, address(offset)));
    }
  }

  @Override
  public double getDouble(int offset) {
    if (NativeMemory.BIG_ENDIAN) {
      return NativeMemory.UNSAFE.getDouble(array, address(offset));
    } else {
      return Double.longBitsToDouble(NativeMemory.UNSAFE.getLong(array, address(offset)));
    }
  }

  @Override
  public void putByte(int offset, byte b) {
    NativeMemory.UNSAFE.putByte(array, address(offset), b);
  }

  @Override
  public void putChar(int offset, char c) {
    if (NativeMemory.BIG_ENDIAN) {
      NativeMemory.UNSAFE.putChar(array, address(offset), c);
    } else {
      NativeMemory.UNSAFE.putChar(array, address(offset), Character.reverseBytes(c));
    }
  }

  @Override
  public void putShort(int offset, short s) {
    if (NativeMemory.BIG_ENDIAN) {
      NativeMemory.UNSAFE.putShort(array, address(offset), s);
    } else {
      NativeMemory.UNSAFE.putShort(array, address(offset), Short.reverseBytes(s));
    }
  }

  @Override
  public void putInt(int offset, int i) {
    if (NativeMemory.BIG_ENDIAN) {
      NativeMemory.UNSAFE.putInt(array, address(offset), i);
    } else {
      NativeMemory.UNSAFE.putInt(array, address(offset), Integer.reverseBytes(i));
    }
  }

  @Override
  public void putLong(int offset, long l) {
    if (NativeMemory.BIG_ENDIAN) {
      NativeMemory.UNSAFE.putLong(array, address(offset), l);
    } else {
      NativeMemory.UNSAFE.putLong(array, address(offset), Long.reverseBytes(l));
    }
  }

  @Override
  public void putFloat(int offset, float f) {
    if (NativeMemory.BIG_ENDIAN) {
      NativeMemory.UNSAFE.putFloat(array, address(offset), f);
    } else {
      NativeMemory.UNSAFE.putInt(array, address(offset), Float.floatToIntBits(f));
    }
  }

  @Override
  public void putDouble(int offset, double d) {
    if (NativeMemory.BIG_ENDIAN) {
      NativeMemory.UNSAFE.putDouble(array, address(offset), d);
    } else {
      NativeMemory.UNSAFE.putLong(array, address(offset), Double.doubleToLongBits(d));
    }
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
