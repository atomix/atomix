/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.util.buffer;

import java.io.DataOutput;

/**
 * Buffer data output wrapper.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferDataOutput extends BufferOutputStream implements DataOutput {
  protected final BufferOutput<?> buffer;

  public BufferDataOutput(BufferOutput<?> buffer) {
    super(buffer);
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    this.buffer = buffer;
  }

  @Override
  public void write(int b) {
    buffer.writeByte(b);
  }

  @Override
  public void write(byte[] b) {
    buffer.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    buffer.write(b, off, len);
  }

  @Override
  public void writeBoolean(boolean b) {
    buffer.writeBoolean(b);
  }

  @Override
  public void writeByte(int b) {
    buffer.writeByte(b);
  }

  @Override
  public void writeShort(int s) {
    buffer.writeShort((short) s);
  }

  @Override
  public void writeChar(int c) {
    buffer.writeChar((char) c);
  }

  @Override
  public void writeInt(int i) {
    buffer.writeInt(i);
  }

  @Override
  public void writeLong(long l) {
    buffer.writeLong(l);
  }

  @Override
  public void writeFloat(float f) {
    buffer.writeFloat(f);
  }

  @Override
  public void writeDouble(double d) {
    buffer.writeDouble(d);
  }

  @Override
  public void writeBytes(String s) {
    buffer.write(s.getBytes());
  }

  @Override
  public void writeChars(String s) {
    for (char c : s.toCharArray()) {
      buffer.writeChar(c);
    }
  }

  @Override
  public void writeUTF(String s) {
    buffer.writeUTF8(s);
  }

}
