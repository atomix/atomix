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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteOrder;

/**
 * Native memory. Represents memory that can be accessed directly via {@link sun.misc.Unsafe}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeMemory implements Memory {
  static final Unsafe UNSAFE;
  private static final boolean UNALIGNED;
  private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

  /**
   * Allocates native memory via {@link DirectMemoryAllocator}.
   *
   * @param size The size of the memory to allocate.
   * @return The allocated memory.
   */
  public static NativeMemory allocate(long size) {
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
  private final long size;
  protected final MemoryAllocator allocator;

  @SuppressWarnings("unchecked")
  protected NativeMemory(long address, long size, MemoryAllocator<? extends NativeMemory> allocator) {
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
  public final long address(long offset) {
    return address + offset;
  }

  /**
   * Returns the address for a byte within an offset.
   */
  private long address(long offset, int b) {
    return address + offset + b;
  }

  @Override
  public long size() {
    return size;
  }

  /**
   * Returns the underlying {@link sun.misc.Unsafe} instance.
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
  public byte getByte(long offset) {
    return UNSAFE.getByte(address(offset));
  }

  private byte getByte(long offset, int pos) {
    return UNSAFE.getByte(address(offset, pos));
  }

  @Override
  public char getChar(long offset) {
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
  public short getShort(long offset) {
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
  public int getInt(long offset) {
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
  public long getLong(long offset) {
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
  public float getFloat(long offset) {
    return Float.intBitsToFloat(getInt(offset));
  }

  @Override
  public double getDouble(long offset) {
    return Double.longBitsToDouble(getLong(offset));
  }

  @Override
  public void putByte(long offset, byte b) {
    UNSAFE.putByte(address(offset), b);
  }

  private void putByte(long offset, int pos, byte b) {
    UNSAFE.putByte(address(offset, pos), b);
  }

  @Override
  public void putChar(long offset, char c) {
    if (UNALIGNED) {
      putChar(address(offset), c);
    } else if (BIG_ENDIAN) {
      putByte(offset, (byte) (c >>> 8));
      putByte(offset, 1, (byte) c);
    } else {
      putByte(offset, 1, (byte) (c >>> 8));
      putByte(offset, (byte) c);
    }
  }

  @Override
  public void putShort(long offset, short s) {
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
  public void putInt(long offset, int i) {
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
  public void putLong(long offset, long l) {
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
  public void putFloat(long offset, float f) {
    putInt(offset, Float.floatToRawIntBits(f));
  }

  @Override
  public void putDouble(long offset, double d) {
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
