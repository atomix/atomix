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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Output stream output.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class OutputStreamBufferOutput implements BufferOutput {
  private final DataOutputStream os;

  public OutputStreamBufferOutput(OutputStream os) {
    this(new DataOutputStream(os));
  }

  public OutputStreamBufferOutput(DataOutputStream os) {
    if (os == null)
      throw new NullPointerException("output stream cannot be null");
    this.os = os;
  }

  @Override
  public BufferOutput write(Bytes bytes) {
    if (bytes instanceof HeapBytes) {
      try {
        os.write(((HeapBytes) bytes).array());
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    } else {
      byte[] buffer = new byte[(int) bytes.size()];
      bytes.read(0, buffer, 0, buffer.length);
      try {
        os.write(buffer);
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    }
    return this;
  }

  @Override
  public BufferOutput write(byte[] bytes) {
    try {
      os.write(bytes);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput write(Bytes bytes, long offset, long length) {
    if (bytes instanceof HeapBytes) {
      try {
        os.write(((HeapBytes) bytes).array(), (int) offset, (int) length);
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    } else {
      byte[] buffer = new byte[(int) bytes.size()];
      bytes.read(0, buffer, 0, buffer.length);
      try {
        os.write(buffer, (int) offset, (int) length);
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    }
    return this;
  }

  @Override
  public BufferOutput write(byte[] bytes, long offset, long length) {
    try {
      os.write(bytes, (int) offset, (int) length);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput write(Buffer buffer) {
    if (buffer instanceof HeapBuffer) {
      try {
        os.write(((HeapBuffer) buffer).array());
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    } else {
      byte[] bytes = new byte[(int) buffer.remaining()];
      buffer.read(bytes);
      try {
        os.write(bytes);
      } catch (IOException e) {
        throw new CopycatIOException(e);
      }
    }
    return this;
  }

  @Override
  public BufferOutput writeByte(int b) {
    try {
      os.writeByte(b);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeUnsignedByte(int b) {
    try {
      os.writeByte(b);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeChar(char c) {
    try {
      os.writeChar(c);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeShort(short s) {
    try {
      os.writeShort(s);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeUnsignedShort(int s) {
    try {
      os.writeShort(s);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeMedium(int m) {
    try {
      os.writeByte((byte) (m >>> 16));
      os.writeByte((byte) (m >>> 8));
      os.writeByte((byte) m);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeUnsignedMedium(int m) {
    return writeMedium(m);
  }

  @Override
  public BufferOutput writeInt(int i) {
    try {
      os.writeInt(i);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeUnsignedInt(long i) {
    try {
      os.writeInt((int) i);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeLong(long l) {
    try {
      os.writeLong(l);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeFloat(float f) {
    try {
      os.writeFloat(f);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeDouble(double d) {
    try {
      os.writeDouble(d);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeBoolean(boolean b) {
    try {
      os.writeBoolean(b);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeString(String s) {
    try {
      os.writeUTF(s);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput writeUTF8(String s) {
    try {
      os.writeUTF(s);
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public BufferOutput flush() {
    try {
      os.flush();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
    return this;
  }

  @Override
  public void close() {
    try {
      os.close();
    } catch (IOException e) {
      throw new CopycatIOException(e);
    }
  }

}
