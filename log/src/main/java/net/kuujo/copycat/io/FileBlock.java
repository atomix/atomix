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

import net.kuujo.copycat.io.util.ReferenceManager;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * File storage block.
 * <p>
 * This class wraps a {@link java.io.RandomAccessFile} instance and delegates all {@link net.kuujo.copycat.io.Buffer}
 * interface calls directly to the underlying {@code RandomAccessFile}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileBlock extends AbstractBlock<FileBlock> {
  private final RandomAccessFile file;
  private final long offset;

  FileBlock(int index, RandomAccessFile file, long offset, long length, ReferenceManager<FileBlock> manager) {
    super(index, length, manager);
    if (file == null)
      throw new NullPointerException("file cannot be null");
    if (offset < 0)
      throw new IllegalArgumentException("offset cannot be negative");
    if (length <= 0)
      throw new IllegalArgumentException("length must be positive");
    this.file = file;
    this.offset = offset;
  }

  /**
   * Seeks to the given offset.
   */
  private void seekToOffset(long offset) throws IOException {
    long realOffset = offset(offset);
    if (file.getFilePointer() != realOffset) {
      file.seek(realOffset);
    }
  }

  /**
   * Returns the real offset for the given relative offset.
   */
  private long offset(long relativeOffset) {
    return offset + relativeOffset;
  }

  @Override
  public Block read(byte[] buffer, long offset, int length) {
    checkRead(offset, length);
    try {
      seekToOffset(offset);
      file.read(buffer, (int) offset, length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block read(byte[] bytes) {
    long offset = getAndSetPosition(checkRead(position(), Byte.BYTES));
    try {
      seekToOffset(offset);
      file.read(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public int readByte() {
    long offset = getAndSetPosition(checkRead(position(), Byte.BYTES));
    try {
      seekToOffset(offset);
      return file.readByte();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readByte(long offset) {
    checkRead(offset, Byte.BYTES);
    try {
      seekToOffset(offset);
      return file.readByte();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public char readChar() {
    long offset = getAndSetPosition(checkRead(position(), Character.BYTES));
    try {
      seekToOffset(offset);
      return file.readChar();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public char readChar(long offset) {
    checkRead(offset, Character.BYTES);
    try {
      seekToOffset(offset);
      return file.readChar();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public short readShort() {
    long offset = getAndSetPosition(checkRead(position(), Short.BYTES));
    try {
      seekToOffset(offset);
      return file.readShort();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public short readShort(long offset) {
    checkRead(offset, Short.BYTES);
    try {
      seekToOffset(offset);
      return file.readShort();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readInt() {
    long offset = getAndSetPosition(checkRead(position(), Integer.BYTES));
    try {
      seekToOffset(offset);
      return file.readInt();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readInt(long offset) {
    checkRead(offset, Integer.BYTES);
    try {
      seekToOffset(offset);
      return file.readInt();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long readLong() {
    long offset = getAndSetPosition(checkRead(position(), Long.BYTES));
    try {
      seekToOffset(offset);
      return file.readLong();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long readLong(long offset) {
    checkRead(offset, Long.BYTES);
    try {
      seekToOffset(offset);
      return file.readLong();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public float readFloat() {
    long offset = getAndSetPosition(checkRead(position(), Float.BYTES));
    try {
      seekToOffset(offset);
      return file.readFloat();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public float readFloat(long offset) {
    checkRead(offset, Float.BYTES);
    try {
      seekToOffset(offset);
      return file.readFloat();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public double readDouble() {
    long offset = getAndSetPosition(checkRead(position(), Double.BYTES));
    try {
      seekToOffset(offset);
      return file.readDouble();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public double readDouble(long offset) {
    checkRead(offset, Double.BYTES);
    try {
      seekToOffset(offset);
      return file.readDouble();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean readBoolean() {
    long offset = getAndSetPosition(checkRead(position(), Byte.BYTES));
    try {
      seekToOffset(offset);
      return file.readBoolean();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean readBoolean(long offset) {
    checkRead(offset, Byte.BYTES);
    try {
      seekToOffset(offset);
      return file.readBoolean();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Block write(byte[] bytes) {
    long offset = checkWrite(position(), bytes.length);
    try {
      seekToOffset(offset);
      file.write(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block write(byte[] bytes, long offset, int length) {
    checkWrite(offset, length);
    try {
      seekToOffset(offset);
      file.write(bytes, (int) offset, length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeByte(int b) {
    long offset = checkRead(position(), Byte.BYTES);
    try {
      seekToOffset(offset);
      file.writeByte(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeByte(long offset, int b) {
    checkWrite(offset, Byte.BYTES);
    try {
      seekToOffset(offset);
      file.writeByte(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeChar(char c) {
    long offset = checkRead(position(), Character.BYTES);
    try {
      seekToOffset(offset);
      file.writeChar(c);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeChar(long offset, char c) {
    checkWrite(offset, Character.BYTES);
    try {
      seekToOffset(offset);
      file.writeChar(c);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeShort(short s) {
    long offset = checkRead(position(), Short.BYTES);
    try {
      seekToOffset(offset);
      file.writeShort(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeShort(long offset, short s) {
    checkWrite(offset, Short.BYTES);
    try {
      seekToOffset(offset);
      file.writeShort(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeInt(int i) {
    long offset = checkRead(position(), Integer.BYTES);
    try {
      seekToOffset(offset);
      file.writeInt(i);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeInt(long offset, int i) {
    checkWrite(offset, Integer.BYTES);
    try {
      seekToOffset(offset);
      file.writeInt(i);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeLong(long l) {
    long offset = checkRead(position(), Long.BYTES);
    try {
      seekToOffset(offset);
      file.writeLong(l);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeLong(long offset, long l) {
    checkWrite(offset, Long.BYTES);
    try {
      seekToOffset(offset);
      file.writeLong(l);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeFloat(float f) {
    long offset = checkRead(position(), Float.BYTES);
    try {
      seekToOffset(offset);
      file.writeFloat(f);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeFloat(long offset, float f) {
    checkWrite(offset, Float.BYTES);
    try {
      seekToOffset(offset);
      file.writeFloat(f);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeDouble(double d) {
    long offset = checkRead(position(), Double.BYTES);
    try {
      seekToOffset(offset);
      file.writeDouble(d);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeDouble(long offset, double d) {
    checkWrite(offset, Double.BYTES);
    try {
      seekToOffset(offset);
      file.writeDouble(d);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeBoolean(boolean b) {
    long offset = checkRead(position(), Byte.BYTES);
    try {
      seekToOffset(offset);
      file.writeBoolean(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Block writeBoolean(long offset, boolean b) {
    checkWrite(offset, Byte.BYTES);
    try {
      seekToOffset(offset);
      file.writeBoolean(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

}
