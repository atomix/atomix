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

import net.kuujo.copycat.io.serializer.Serializer;

import java.io.IOException;
import java.io.ObjectInput;

/**
 * Buffer object input.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferObjectInput extends BufferDataInput implements ObjectInput {
  private final Serializer serializer;

  public BufferObjectInput(BufferInput buffer, Serializer serializer) {
    super(buffer);
    if (serializer == null)
      throw new NullPointerException("serializer cannot be null");
    this.serializer = serializer;
  }

  @Override
  public Object readObject() throws ClassNotFoundException, IOException {
    return serializer.readObject(buffer);
  }

  @Override
  public int read() throws IOException {
    return buffer.readByte();
  }

  @Override
  public int read(byte[] b) throws IOException {
    int i = 0;
    while (i < b.length && buffer.hasRemaining()) {
      b[i++] = (byte) buffer.readByte();
    }
    return i;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int i = 0;
    while (i < len && buffer.hasRemaining()) {
      b[i + off] = (byte) buffer.readByte();
    }
    return i;
  }

  @Override
  public long skip(long n) throws IOException {
    long skipped = Math.min(n, buffer.remaining());
    buffer.skip(skipped);
    return skipped;
  }

  @Override
  public int available() throws IOException {
    return (int) buffer.remaining();
  }

  @Override
  public void close() throws IOException {
    buffer.close();
  }

}
