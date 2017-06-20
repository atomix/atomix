/*
 * Copyright 2015-present Open Networking Laboratory
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
package io.atomix.storage.buffer;

import io.atomix.utils.AtomixIOException;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static io.atomix.storage.buffer.Bytes.BOOLEAN;
import static io.atomix.storage.buffer.Bytes.BYTE;
import static io.atomix.storage.buffer.Bytes.CHARACTER;
import static io.atomix.storage.buffer.Bytes.DOUBLE;
import static io.atomix.storage.buffer.Bytes.FLOAT;
import static io.atomix.storage.buffer.Bytes.INTEGER;
import static io.atomix.storage.buffer.Bytes.LONG;
import static io.atomix.storage.buffer.Bytes.MEDIUM;
import static io.atomix.storage.buffer.Bytes.SHORT;

/**
 * Input stream buffer input.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InputStreamBufferInput implements BufferInput<BufferInput<?>> {
  private final DataInputStream is;
  private long position;

  public InputStreamBufferInput(InputStream is) {
    this(new DataInputStream(is));
  }

  public InputStreamBufferInput(DataInputStream is) {
    if (is == null)
      throw new NullPointerException("input stream cannot be null");
    this.is = is;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public long remaining() {
    try {
      return is.available();
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public boolean hasRemaining() {
    return remaining() > 0;
  }

  @Override
  public BufferInput<?> skip(long bytes) {
    try {
      position += is.skip(bytes);
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
    return this;
  }

  @Override
  public BufferInput<?> read(Bytes bytes) {
    if (bytes instanceof UnsafeHeapBytes) {
      try {
        position += is.read(bytes.array());
      } catch (IOException e) {
        throw new AtomixIOException(e);
      }
    } else {
      byte[] buffer = new byte[(int) bytes.size()];
      try {
        int read = is.read(buffer);
        if (read != -1) {
          bytes.write(0, buffer, 0, read);
        }
      } catch (IOException e) {
        throw new AtomixIOException(e);
      }
    }
    return this;
  }

  @Override
  public BufferInput<?> read(byte[] bytes) {
    try {
      position += is.read(bytes);
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
    return this;
  }

  @Override
  public BufferInput<?> read(Bytes bytes, long offset, long length) {
    if (bytes instanceof UnsafeHeapBytes) {
      try {
        position += is.read(bytes.array(), (int) offset, (int) length);
      } catch (IOException e) {
        throw new AtomixIOException(e);
      }
    } else {
      byte[] buffer = new byte[1024];
      try {
        long position = offset;
        long remaining = length;
        int read;
        while ((read = is.read(buffer)) != -1) {
          bytes.write(position, buffer, 0, Math.min(read, remaining));
          position += read;
          remaining -= read;
        }
      } catch (IOException e) {
        throw new AtomixIOException(e);
      }
    }
    return this;
  }

  @Override
  public BufferInput<?> read(byte[] bytes, long offset, long length) {
    try {
      position += is.read(bytes, (int) offset, (int) length);
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
    return this;
  }

  @Override
  public BufferInput<?> read(Buffer buffer) {
    if (buffer.hasArray()) {
      try {
        position += is.read(buffer.array());
      } catch (IOException e) {
        throw new AtomixIOException(e);
      }
    } else {
      byte[] bytes = new byte[1024];
      try {
        int read;
        while ((read = is.read(bytes)) != -1) {
          buffer.write(bytes, 0, read);
        }
      } catch (IOException e) {
        throw new AtomixIOException(e);
      }
    }
    return this;
  }

  @Override
  public int readByte() {
    try {
      int value = is.readByte();
      position += BYTE;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public int readUnsignedByte() {
    try {
      int value = is.readUnsignedByte();
      position += BYTE;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public char readChar() {
    try {
      char value = is.readChar();
      position += CHARACTER;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public short readShort() {
    try {
      short value = is.readShort();
      position += SHORT;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public int readUnsignedShort() {
    try {
      int value = is.readUnsignedShort();
      position += SHORT;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public int readMedium() {
    try {
      int value = is.readByte() << 16
          | (is.readByte() & 0xff) << 8
          | (is.readByte() & 0xff);
      position += MEDIUM;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public int readUnsignedMedium() {
    try {
      int value = (is.readByte() & 0xff) << 16
          | (is.readByte() & 0xff) << 8
          | (is.readByte() & 0xff);
      position += MEDIUM;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public int readInt() {
    try {
      int value = is.readInt();
      position += INTEGER;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public long readUnsignedInt() {
    try {
      long value = is.readInt() & 0xFFFFFFFFL;
      position += INTEGER;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public long readLong() {
    try {
      long value = is.readLong();
      position += LONG;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public float readFloat() {
    try {
      float value = is.readFloat();
      position += FLOAT;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public double readDouble() {
    try {
      double value = is.readDouble();
      position += DOUBLE;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public boolean readBoolean() {
    try {
      boolean value = is.readBoolean();
      position += BOOLEAN;
      return value;
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  @Override
  public String readString() {
    return readString(Charset.defaultCharset());
  }

  @Override
  public String readString(Charset charset) {
    if (readBoolean()) {
      byte[] bytes = new byte[readUnsignedShort()];
      try {
        position += BOOLEAN + SHORT + is.read(bytes, 0, bytes.length);
        return new String(bytes, charset);
      } catch (IOException e) {
        throw new AtomixIOException(e);
      }
    } else {
      position += BOOLEAN;
    }
    return null;
  }

  @Override
  public String readUTF8() {
    return readString(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    try {
      is.close();
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

}
