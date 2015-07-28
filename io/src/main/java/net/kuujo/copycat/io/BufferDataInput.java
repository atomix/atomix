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

import java.io.DataInput;
import java.io.IOException;

/**
 * Buffer data input.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferDataInput implements DataInput {
  protected final BufferInput buffer;

  public BufferDataInput(BufferInput buffer) {
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    this.buffer = buffer;
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    buffer.read(b);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    buffer.read(b, off, len);
  }

  @Override
  public int skipBytes(int n) {
    int skipped = Math.min(n, (int) buffer.remaining());
    buffer.skip(skipped);
    return skipped;
  }

  @Override
  public boolean readBoolean() throws IOException {
    return buffer.readBoolean();
  }

  @Override
  public byte readByte() throws IOException {
    return (byte) buffer.readByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return buffer.readUnsignedByte();
  }

  @Override
  public short readShort() throws IOException {
    return buffer.readShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return buffer.readUnsignedShort();
  }

  @Override
  public char readChar() throws IOException {
    return buffer.readChar();
  }

  @Override
  public int readInt() throws IOException {
    return buffer.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return buffer.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return buffer.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return buffer.readDouble();
  }

  @Override
  public String readLine() throws IOException {
    return buffer.readUTF8();
  }

  @Override
  public String readUTF() throws IOException {
    return buffer.readUTF8();
  }

}
