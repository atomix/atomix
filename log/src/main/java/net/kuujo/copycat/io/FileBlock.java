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
 * File storage block.
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
    checkBounds(offset);
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
  public char readChar(long offset) {
    try {
      seekToOffset(offset);
      return file.readChar();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public short readShort(long offset) {
    try {
      seekToOffset(offset);
      return file.readShort();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readInt(long offset) {
    try {
      seekToOffset(offset);
      return file.readInt();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long readLong(long offset) {
    try {
      seekToOffset(offset);
      return file.readLong();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public float readFloat(long offset) {
    try {
      seekToOffset(offset);
      return file.readFloat();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public double readDouble(long offset) {
    try {
      seekToOffset(offset);
      return file.readDouble();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean readBoolean(long offset) {
    try {
      seekToOffset(offset);
      return file.readBoolean();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Buffer writeChar(long offset, char c) {
    try {
      seekToOffset(offset);
      file.writeChar(c);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public FileBlock writeShort(long offset, short s) {
    try {
      seekToOffset(offset);
      file.writeShort(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public FileBlock writeInt(long offset, int i) {
    try {
      seekToOffset(offset);
      file.writeInt(i);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public FileBlock writeLong(long offset, long l) {
    try {
      seekToOffset(offset);
      file.writeLong(l);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public FileBlock writeFloat(long offset, float f) {
    try {
      seekToOffset(offset);
      file.writeFloat(f);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public FileBlock writeDouble(long offset, double d) {
    try {
      seekToOffset(offset);
      file.writeDouble(d);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Buffer writeBoolean(long offset, boolean b) {
    try {
      seekToOffset(offset);
      file.writeBoolean(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

}
