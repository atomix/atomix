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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteOrder;

/**
 * Native memory. Represents memory that can be accessed directly via {@link Unsafe}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeMemory implements Memory {
  static final Unsafe UNSAFE;
  private static final boolean UNALIGNED;
  static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

  /**
   * Allocates native memory via {@link DirectMemoryAllocator}.
   *
   * @param size The count of the memory to allocate.
   * @return The allocated memory.
   */
  public static NativeMemory allocate(int size) {
    return new DirectMemoryAllocator().allocate(size);
  }

  static {
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      UNSAFE = (Unsafe) unsafeField.get(null);
    } catch (Exception e) {
      throw new AssertionError();
    }

    boolean unaligned;
    try {
      Class<?> bitsClass = Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
      Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
      unalignedMethod.setAccessible(true);
      unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      unaligned = false;
    }
    UNALIGNED = unaligned;
  }

  private long address;
  private final int size;
  protected final MemoryAllocator allocator;

  @SuppressWarnings("unchecked")
  protected NativeMemory(long address, int size, MemoryAllocator<? extends NativeMemory> allocator) {
    if (allocator == null)
      throw new NullPointerException("allocator cannot be null");
    this.address = address;
    this.size = size;
    this.allocator = allocator;
  }

  @Override
  @SuppressWarnings("unchecked")
  public MemoryAllocator<NativeMemory> allocator() {
    return allocator;
  }

  @Override
  public final long address() {
    return address;
  }

  @Override
  public final long address(int offset) {
    return address + offset;
  }

  /**
   * Returns the address for a byte within an offset.
   */
  private long address(int offset, int b) {
    return address + offset + b;
  }

  @Override
  public int size() {
    return size;
  }

  /**
   * Returns the underlying {@link Unsafe} instance.
   *
   * @return The underlying unsafe memory instance.
   */
  public final Unsafe unsafe() {
    return UNSAFE;
  }

  @Override
  public NativeMemory copy() {
    NativeMemory memory = (NativeMemory) allocator.allocate(size);
    UNSAFE.copyMemory(address, memory.address, size);
    return memory;
  }

  @Override
  public byte getByte(int offset) {
    return UNSAFE.getByte(address(offset));
  }

  private byte getByte(int offset, int pos) {
    return UNSAFE.getByte(address(offset, pos));
  }

  @Override
  public char getChar(int offset) {
    if (UNALIGNED) {
      return UNSAFE.getChar(address(offset));
    } else if (BIG_ENDIAN) {
      return (char) (getByte(offset) << 8
          | getByte(offset, 1) & 0xff);
    } else {
      return (char) ((getByte(offset, 1) << 8)
          | getByte(offset) & 0xff);
    }
  }

  @Override
  public short getShort(int offset) {
    if (UNALIGNED) {
      return UNSAFE.getShort(address(offset));
    } else if (BIG_ENDIAN) {
      return (short) (getByte(offset) << 8
          | getByte(offset, 1) & 0xff);
    } else {
      return (short) ((getByte(offset, 1) << 8)
          | getByte(offset) & 0xff);
    }
  }

  @Override
  public int getInt(int offset) {
    if (UNALIGNED) {
      return UNSAFE.getInt(address(offset));
    } else if (BIG_ENDIAN) {
      return (getByte(offset, 0)) << 24
          | (getByte(offset, 1) & 0xff) << 16
          | (getByte(offset, 2) & 0xff) << 8
          | (getByte(offset, 3) & 0xff);
    } else {
      return (getByte(offset, 3)) << 24
          | (getByte(offset, 2) & 0xff) << 16
          | (getByte(offset, 1) & 0xff) << 8
          | (getByte(offset, 0) & 0xff);
    }
  }

  @Override
  public long getLong(int offset) {
    if (UNALIGNED) {
      return UNSAFE.getLong(address(offset));
    } else if (BIG_ENDIAN) {
      return ((long) getByte(offset)) << 56
          | ((long) getByte(offset, 1) & 0xff) << 48
          | ((long) getByte(offset, 2) & 0xff) << 40
          | ((long) getByte(offset, 3) & 0xff) << 32
          | ((long) getByte(offset, 4) & 0xff) << 24
          | ((long) getByte(offset, 5) & 0xff) << 16
          | ((long) getByte(offset, 6) & 0xff) << 8
          | ((long) getByte(offset, 7) & 0xff);
    } else {
      return ((long) getByte(offset, 7)) << 56
          | ((long) getByte(offset, 6) & 0xff) << 48
          | ((long) getByte(offset, 5) & 0xff) << 40
          | ((long) getByte(offset, 4) & 0xff) << 32
          | ((long) getByte(offset, 3) & 0xff) << 24
          | ((long) getByte(offset, 2) & 0xff) << 16
          | ((long) getByte(offset, 1) & 0xff) << 8
          | ((long) getByte(offset) & 0xff);
    }
  }

  @Override
  public float getFloat(int offset) {
    return Float.intBitsToFloat(getInt(offset));
  }

  @Override
  public double getDouble(int offset) {
    return Double.longBitsToDouble(getLong(offset));
  }

  @Override
  public void putByte(int offset, byte b) {
    UNSAFE.putByte(address(offset), b);
  }

  private void putByte(int offset, int pos, byte b) {
    UNSAFE.putByte(address(offset, pos), b);
  }

  @Override
  public void putChar(int offset, char c) {
    if (UNALIGNED) {
      UNSAFE.putChar(offset, c);
    } else if (BIG_ENDIAN) {
      putByte(offset, (byte) (c >>> 8));
      putByte(offset, 1, (byte) c);
    } else {
      putByte(offset, 1, (byte) (c >>> 8));
      putByte(offset, (byte) c);
    }
  }

  @Override
  public void putShort(int offset, short s) {
    if (UNALIGNED) {
      UNSAFE.putShort(address(offset), s);
    } else if (BIG_ENDIAN) {
      putByte(offset, (byte) (s >>> 8));
      putByte(offset, 1, (byte) s);
    } else {
      putByte(offset, 1, (byte) (s >>> 8));
      putByte(offset, (byte) s);
    }
  }

  @Override
  public void putInt(int offset, int i) {
    if (UNALIGNED) {
      UNSAFE.putInt(address(offset), i);
    } else if (BIG_ENDIAN) {
      putByte(offset, (byte) (i >>> 24));
      putByte(offset, 1, (byte) (i >>> 16));
      putByte(offset, 2, (byte) (i >>> 8));
      putByte(offset, 3, (byte) i);
    } else {
      putByte(offset, 3, (byte) (i >>> 24));
      putByte(offset, 2, (byte) (i >>> 16));
      putByte(offset, 1, (byte) (i >>> 8));
      putByte(offset, (byte) i);
    }
  }

  @Override
  public void putLong(int offset, long l) {
    if (UNALIGNED) {
      UNSAFE.putLong(address(offset), l);
    } else if (BIG_ENDIAN) {
      putByte(offset, (byte) (l >>> 56));
      putByte(offset, 1, (byte) (l >>> 48));
      putByte(offset, 2, (byte) (l >>> 40));
      putByte(offset, 3, (byte) (l >>> 32));
      putByte(offset, 4, (byte) (l >>> 24));
      putByte(offset, 5, (byte) (l >>> 16));
      putByte(offset, 6, (byte) (l >>> 8));
      putByte(offset, 7, (byte) l);
    } else {
      putByte(offset, 7, (byte) (l >>> 56));
      putByte(offset, 6, (byte) (l >>> 48));
      putByte(offset, 5, (byte) (l >>> 40));
      putByte(offset, 4, (byte) (l >>> 32));
      putByte(offset, 3, (byte) (l >>> 24));
      putByte(offset, 2, (byte) (l >>> 16));
      putByte(offset, 1, (byte) (l >>> 8));
      putByte(offset, (byte) l);
    }
  }

  @Override
  public void putFloat(int offset, float f) {
    putInt(offset, Float.floatToRawIntBits(f));
  }

  @Override
  public void putDouble(int offset, double d) {
    putLong(offset, Double.doubleToRawLongBits(d));
  }

  @Override
  public void clear() {
    UNSAFE.setMemory(address, size, (byte) 0);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void free() {
    if (address != 0) {
      NativeMemory.UNSAFE.freeMemory(address);
      address = 0;
    }
  }

}
