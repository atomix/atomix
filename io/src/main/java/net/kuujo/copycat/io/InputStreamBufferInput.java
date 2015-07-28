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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Input stream buffer input.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InputStreamBufferInput implements BufferInput {
  private final DataInputStream is;

  public InputStreamBufferInput(InputStream is) {
    this(new DataInputStream(is));
  }

  public InputStreamBufferInput(DataInputStream is) {
    if (is == null)
      throw new NullPointerException("input stream cannot be null");
    this.is = is;
  }

  @Override
  public long remaining() {
    try {
      return is.available();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public boolean hasRemaining() {
    return remaining() > 0;
  }

  @Override
  public BufferInput skip(long bytes) {
    try {
      is.skip(bytes);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferInput read(Bytes bytes) {
    if (bytes instanceof HeapBytes) {
      try {
        is.read(((HeapBytes) bytes).array());
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    } else {
      byte[] buffer = new byte[(int) bytes.size()];
      try {
        int read = is.read(buffer);
        if (read != -1) {
          bytes.write(0, buffer, 0, read);
        }
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    }
    return this;
  }

  @Override
  public BufferInput read(byte[] bytes) {
    try {
      is.read(bytes);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferInput read(Bytes bytes, long offset, long length) {
    if (bytes instanceof HeapBytes) {
      try {
        is.read(((HeapBytes) bytes).array(), (int) offset, (int) length);
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    } else {
      byte[] buffer = new byte[1024];
      try {
        long position = offset;
        long remaining = length;
        int read = -1;
        while ((read = is.read(buffer)) != -1) {
          bytes.write(position, buffer, 0, Math.min(read, remaining));
          position += read;
          remaining -= read;
        }
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    }
    return this;
  }

  @Override
  public BufferInput read(byte[] bytes, long offset, long length) {
    try {
      is.read(bytes, (int) offset, (int) length);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferInput read(Buffer buffer) {
    if (buffer instanceof HeapBuffer) {
      try {
        is.read(((HeapBuffer) buffer).array());
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    } else {
      byte[] bytes = new byte[1024];
      try {
        int read = -1;
        while ((read = is.read(bytes)) != -1) {
          buffer.write(bytes, 0, read);
        }
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    }
    return this;
  }

  @Override
  public int readByte() {
    try {
      return is.readByte();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public int readUnsignedByte() {
    try {
      return is.readUnsignedByte();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public char readChar() {
    try {
      return is.readChar();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public short readShort() {
    try {
      return is.readShort();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public int readUnsignedShort() {
    try {
      return is.readUnsignedShort();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public int readMedium() {
    try {
      return is.readByte() << 16
        | (is.readByte() & 0xff) << 8
        | (is.readByte() & 0xff);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public int readUnsignedMedium() {
    try {
      return (is.readByte() & 0xff) << 16
        | (is.readByte() & 0xff) << 8
        | (is.readByte() & 0xff);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public int readInt() {
    try {
      return is.readInt();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public long readUnsignedInt() {
    try {
      return is.readInt() & 0xFFFFFFFFL;
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public long readLong() {
    try {
      return is.readLong();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public float readFloat() {
    try {
      return is.readFloat();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public double readDouble() {
    try {
      return is.readDouble();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public boolean readBoolean() {
    try {
      return is.readBoolean();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public String readString() {
    try {
      return is.readUTF();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public String readUTF8() {
    try {
      return is.readUTF();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      is.close();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

}
