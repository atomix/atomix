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

import net.kuujo.copycat.io.util.MappedMemoryAllocator;
import net.kuujo.copycat.io.util.Memory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * File bytes.
 * <p>
 * File bytes wrap a simple {@link java.io.RandomAccessFile} instance to provide random access to a randomAccessFile on local disk. All
 * operations are delegated directly to the {@link java.io.RandomAccessFile} interface, and limitations are dependent on the
 * semantics of the underlying randomAccessFile.
 * <p>
 * Bytes are always stored in the underlying randomAccessFile in {@link java.nio.ByteOrder#BIG_ENDIAN} order.
 * To flip the byte order to read or write to/from a randomAccessFile in {@link java.nio.ByteOrder#LITTLE_ENDIAN} order use
 * {@link Bytes#order(java.nio.ByteOrder)}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileBytes extends AbstractBytes {
  static final String DEFAULT_MODE = "rw";

  /**
   * Allocates a randomAccessFile buffer of unlimited count.
   * <p>
   * The buffer will be allocated with {@link Long#MAX_VALUE} bytes. As bytes are written to the buffer, the underlying
   * {@link java.io.RandomAccessFile} will expand.
   *
   * @param file The randomAccessFile to allocate.
   * @return The allocated buffer.
   */
  public static FileBytes allocate(File file) {
    return allocate(file, DEFAULT_MODE, Long.MAX_VALUE);
  }

  /**
   * Allocates a randomAccessFile buffer.
   * <p>
   * If the underlying randomAccessFile is empty, the randomAccessFile count will expand dynamically as bytes are written to the randomAccessFile.
   *
   * @param file The randomAccessFile to allocate.
   * @param size The count of the bytes to allocate.
   * @return The allocated buffer.
   */
  public static FileBytes allocate(File file, long size) {
    return allocate(file, DEFAULT_MODE, size);
  }

  /**
   * Allocates a randomAccessFile buffer.
   * <p>
   * If the underlying randomAccessFile is empty, the randomAccessFile count will expand dynamically as bytes are written to the randomAccessFile.
   *
   * @param file The randomAccessFile to allocate.
   * @param mode The mode in which to open the underlying {@link java.io.RandomAccessFile}.
   * @param size The count of the bytes to allocate.
   * @return The allocated buffer.
   */
  public static FileBytes allocate(File file, String mode, long size) {
    return new FileBytes(file, mode, Memory.Util.toPow2(size));
  }

  private final File file;
  private final RandomAccessFile randomAccessFile;
  private long size;

  FileBytes(File file, String mode, long size) {
    if (file == null)
      throw new NullPointerException("file cannot be null");
    if (mode == null)
      mode = DEFAULT_MODE;
    if (size < 0)
      throw new IllegalArgumentException("size must be positive");

    this.file = file;
    this.size = size;
    try {
      this.randomAccessFile = new RandomAccessFile(file, mode);
      if (size > randomAccessFile.length())
        randomAccessFile.setLength(size);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the underlying file object.
   *
   * @return The underlying file.
   */
  public File file() {
    return file;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public Bytes resize(long newSize) {
    if (newSize < size)
      throw new IllegalArgumentException("cannot decrease file bytes size; use zero() to decrease file size");
    this.size = newSize;
    try {
      long length = randomAccessFile.length();
      if (size > length)
        randomAccessFile.setLength(newSize);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public boolean isFile() {
    return true;
  }

  /**
   * Maps a portion of the randomAccessFile into memory in {@link java.nio.channels.FileChannel.MapMode#READ_WRITE} mode and returns
   * a {@link MappedBytes} instance.
   *
   * @param offset The offset from which to map the randomAccessFile into memory.
   * @param size The count of the bytes to map into memory.
   * @return The mapped bytes.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed
   *         {@link java.nio.MappedByteBuffer} count: {@link Integer#MAX_VALUE}
   */
  public MappedBytes map(long offset, long size) {
    return map(offset, size, FileChannel.MapMode.READ_WRITE);
  }

  /**
   * Maps a portion of the randomAccessFile into memory and returns a {@link MappedBytes} instance.
   *
   * @param offset The offset from which to map the randomAccessFile into memory.
   * @param size The count of the bytes to map into memory.
   * @param mode The mode in which to map the randomAccessFile into memory.
   * @return The mapped bytes.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed
   *         {@link java.nio.MappedByteBuffer} count: {@link Integer#MAX_VALUE}
   */
  public MappedBytes map(long offset, long size, FileChannel.MapMode mode) {
    return new MappedBytes(file, new MappedMemoryAllocator(randomAccessFile, mode, offset).allocate(size));
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.BIG_ENDIAN;
  }

  /**
   * Seeks to the given offset.
   */
  private void seekToOffset(long offset) throws IOException {
    if (randomAccessFile.getFilePointer() != offset) {
      randomAccessFile.seek(offset);
    }
  }

  @Override
  public Bytes zero() {
    try {
      randomAccessFile.setLength(0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes zero(long offset) {
    try {
      randomAccessFile.setLength(offset);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes zero(long offset, long length) {
    for (long i = offset; i < offset + length; i++) {
      writeByte(i, (byte) 0);
    }
    return this;
  }

  @Override
  public Bytes read(long position, Bytes bytes, long offset, long length) {
    checkRead(position, length);
    if (bytes instanceof WrappedBytes)
      bytes = ((WrappedBytes) bytes).root();
    try {
      seekToOffset(position);
      for (long i = 0; i < length; i++) {
        bytes.writeByte(offset + i, randomAccessFile.readByte());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes read(long position, byte[] bytes, long offset, long length) {
    checkRead(position, length);
    try {
      seekToOffset(position);
      randomAccessFile.read(bytes, (int) offset, (int) length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public int readByte(long offset) {
    checkRead(offset, Bytes.BYTE);
    try {
      seekToOffset(offset);
      return randomAccessFile.readByte();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readUnsignedByte(long offset) {
    checkRead(offset, Bytes.BYTE);
    try {
      seekToOffset(offset);
      return randomAccessFile.readUnsignedByte();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public char readChar(long offset) {
    checkRead(offset, Bytes.CHARACTER);
    try {
      seekToOffset(offset);
      return randomAccessFile.readChar();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public short readShort(long offset) {
    checkRead(offset, Bytes.SHORT);
    try {
      seekToOffset(offset);
      return randomAccessFile.readShort();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readUnsignedShort(long offset) {
    checkRead(offset, Bytes.SHORT);
    try {
      seekToOffset(offset);
      return randomAccessFile.readUnsignedShort();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readMedium(long offset) {
    checkRead(offset, Bytes.MEDIUM);
    try {
      seekToOffset(offset);
      return (randomAccessFile.readByte()) << 16
        | (randomAccessFile.readByte() & 0xff) << 8
        | (randomAccessFile.readByte() & 0xff);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readUnsignedMedium(long offset) {
    checkRead(offset, Bytes.MEDIUM);
    try {
      seekToOffset(offset);
      return (randomAccessFile.readByte() & 0xff) << 16
        | (randomAccessFile.readByte() & 0xff) << 8
        | (randomAccessFile.readByte() & 0xff);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readInt(long offset) {
    checkRead(offset, Bytes.INTEGER);
    try {
      seekToOffset(offset);
      return randomAccessFile.readInt();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long readUnsignedInt(long offset) {
    checkRead(offset, Bytes.INTEGER);
    try {
      seekToOffset(offset);
      return randomAccessFile.readInt() & 0xFFFFFFFFL;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long readLong(long offset) {
    checkRead(offset, Bytes.LONG);
    try {
      seekToOffset(offset);
      return randomAccessFile.readLong();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public float readFloat(long offset) {
    checkRead(offset, Bytes.FLOAT);
    try {
      seekToOffset(offset);
      return randomAccessFile.readFloat();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public double readDouble(long offset) {
    checkRead(offset, Bytes.DOUBLE);
    try {
      seekToOffset(offset);
      return randomAccessFile.readDouble();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean readBoolean(long offset) {
    checkRead(offset, Bytes.BOOLEAN);
    try {
      seekToOffset(offset);
      return randomAccessFile.readBoolean();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String readString(long offset) {
    if (readByte(offset) != 0) {
      byte[] bytes = new byte[readUnsignedShort(offset + Bytes.BYTE)];
      read(offset + Bytes.BYTE + Bytes.SHORT, bytes, 0, bytes.length);
      return new String(bytes);
    }
    return null;
  }

  @Override
  public String readUTF8(long offset) {
    if (readByte(offset) != 0) {
      byte[] bytes = new byte[readUnsignedShort(offset + Bytes.BYTE)];
      read(offset + Bytes.BYTE + Bytes.SHORT, bytes, 0, bytes.length);
      return new String(bytes, StandardCharsets.UTF_8);
    }
    return null;
  }

  @Override
  public Bytes write(long position, Bytes bytes, long offset, long length) {
    checkWrite(position, length);
    try {
      seekToOffset(position);
      for (long i = 0; i < length; i++) {
        randomAccessFile.writeByte(bytes.readByte(offset + i));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes write(long position, byte[] bytes, long offset, long length) {
    checkWrite(position, length);
    try {
      seekToOffset(position);
      randomAccessFile.write(bytes, (int) offset, (int) length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeByte(long offset, int b) {
    checkWrite(offset, Bytes.BYTE);
    try {
      seekToOffset(offset);
      randomAccessFile.writeByte(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeUnsignedByte(long offset, int b) {
    checkWrite(offset, Bytes.BYTE);
    try {
      seekToOffset(offset);
      randomAccessFile.writeByte((byte) b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeChar(long offset, char c) {
    checkWrite(offset, Bytes.CHARACTER);
    try {
      seekToOffset(offset);
      randomAccessFile.writeChar(c);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeShort(long offset, short s) {
    checkWrite(offset, Bytes.SHORT);
    try {
      seekToOffset(offset);
      randomAccessFile.writeShort(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeUnsignedShort(long offset, int s) {
    checkWrite(offset, Bytes.SHORT);
    try {
      seekToOffset(offset);
      randomAccessFile.writeShort((short) s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeMedium(long offset, int m) {
    checkWrite(offset, Bytes.SHORT);
    try {
      seekToOffset(offset);
      randomAccessFile.writeByte((byte) (m >>> 16));
      randomAccessFile.writeByte((byte) (m >>> 8));
      randomAccessFile.writeByte((byte) m);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeUnsignedMedium(long offset, int m) {
    return writeMedium(offset, m);
  }

  @Override
  public Bytes writeInt(long offset, int i) {
    checkWrite(offset, Bytes.INTEGER);
    try {
      seekToOffset(offset);
      randomAccessFile.writeInt(i);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeUnsignedInt(long offset, long i) {
    checkWrite(offset, Bytes.INTEGER);
    try {
      seekToOffset(offset);
      randomAccessFile.writeInt((int) i);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeLong(long offset, long l) {
    checkWrite(offset, Bytes.LONG);
    try {
      seekToOffset(offset);
      randomAccessFile.writeLong(l);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeFloat(long offset, float f) {
    checkWrite(offset, Bytes.FLOAT);
    try {
      seekToOffset(offset);
      randomAccessFile.writeFloat(f);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeDouble(long offset, double d) {
    checkWrite(offset, Bytes.DOUBLE);
    try {
      seekToOffset(offset);
      randomAccessFile.writeDouble(d);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeBoolean(long offset, boolean b) {
    checkWrite(offset, Bytes.BOOLEAN);
    try {
      seekToOffset(offset);
      randomAccessFile.writeBoolean(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeString(long offset, String s) {
    if (s == null) {
      return writeByte(offset, 0);
    } else {
      writeByte(offset, 1);
      byte[] bytes = s.getBytes();
      return writeUnsignedShort(offset + Bytes.BYTE, bytes.length)
        .write(offset + Bytes.BYTE + Bytes.SHORT, bytes, 0, bytes.length);
    }
  }

  @Override
  public Bytes writeUTF8(long offset, String s) {
    if (s == null) {
      return writeByte(offset, 0);
    } else {
      writeByte(offset, 1);
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      return writeUnsignedShort(offset + Bytes.BYTE, bytes.length)
        .write(offset + Bytes.BYTE + Bytes.SHORT, bytes, 0, bytes.length);
    }
  }

  @Override
  public Bytes flush() {
    try {
      randomAccessFile.getFD().sync();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public void close() {
    try {
      randomAccessFile.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    super.close();
  }

  /**
   * Deletes the underlying file.
   */
  public void delete() {
    try {
      Files.delete(file.toPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
