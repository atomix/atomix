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

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * File bytes.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileBytes implements Bytes {
  private final RandomAccessFile file;
  private final BufferNavigator navigator;
  private final long offset;
  private final long length;

  public FileBytes(RandomAccessFile file, long offset, long length) {
    this.file = file;
    this.offset = offset;
    this.length = length;
    this.navigator = new BufferNavigator(offset, length);
  }

  @Override
  public long size() {
    return length;
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
  public Bytes read(byte[] bytes, long offset, int length) {
    navigator.checkRead(offset, length);
    try {
      seekToOffset(offset);
      file.read(bytes, (int) offset, length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public int readByte(long offset) {
    navigator.checkRead(offset, Byte.BYTES);
    try {
      seekToOffset(offset);
      return file.readByte();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public char readChar(long offset) {
    navigator.checkRead(offset, Character.BYTES);
    try {
      seekToOffset(offset);
      return file.readChar();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public short readShort(long offset) {
    navigator.checkRead(offset, Short.BYTES);
    try {
      seekToOffset(offset);
      return file.readShort();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readInt(long offset) {
    navigator.checkRead(offset, Integer.BYTES);
    try {
      seekToOffset(offset);
      return file.readInt();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long readLong(long offset) {
    navigator.checkRead(offset, Long.BYTES);
    try {
      seekToOffset(offset);
      return file.readLong();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public float readFloat(long offset) {
    navigator.checkRead(offset, Float.BYTES);
    try {
      seekToOffset(offset);
      return file.readFloat();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public double readDouble(long offset) {
    navigator.checkRead(offset, Double.BYTES);
    try {
      seekToOffset(offset);
      return file.readDouble();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean readBoolean(long offset) {
    navigator.checkRead(offset, Byte.BYTES);
    try {
      seekToOffset(offset);
      return file.readBoolean();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Bytes write(byte[] bytes, long offset, int length) {
    navigator.checkWrite(offset, length);
    try {
      seekToOffset(offset);
      file.write(bytes, (int) offset, length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeByte(long offset, int b) {
    navigator.checkWrite(offset, Byte.BYTES);
    try {
      seekToOffset(offset);
      file.writeByte(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeChar(long offset, char c) {
    navigator.checkWrite(offset, Character.BYTES);
    try {
      seekToOffset(offset);
      file.writeChar(c);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeShort(long offset, short s) {
    navigator.checkWrite(offset, Short.BYTES);
    try {
      seekToOffset(offset);
      file.writeShort(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeInt(long offset, int i) {
    navigator.checkWrite(offset, Integer.BYTES);
    try {
      seekToOffset(offset);
      file.writeInt(i);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeLong(long offset, long l) {
    navigator.checkWrite(offset, Long.BYTES);
    try {
      seekToOffset(offset);
      file.writeLong(l);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeFloat(long offset, float f) {
    navigator.checkWrite(offset, Float.BYTES);
    try {
      seekToOffset(offset);
      file.writeFloat(f);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeDouble(long offset, double d) {
    navigator.checkWrite(offset, Double.BYTES);
    try {
      seekToOffset(offset);
      file.writeDouble(d);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Bytes writeBoolean(long offset, boolean b) {
    navigator.checkWrite(offset, Byte.BYTES);
    try {
      seekToOffset(offset);
      file.writeBoolean(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

}
