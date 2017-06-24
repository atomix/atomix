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

import java.io.IOException;
import java.io.InputStream;

/**
 * Buffer input stream.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class BufferInputStream extends InputStream {
  private final BufferInput<?> buffer;

  public BufferInputStream(BufferInput<?> buffer) {
    this.buffer = buffer;
  }

  @Override
  public int read() throws IOException {
    if (buffer.hasRemaining()) {
      return buffer.readByte();
    }
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    if (buffer.hasRemaining()) {
      int read = Math.min(b.length, (int) buffer.remaining());
      buffer.read(b);
      return read;
    }
    return -1;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int read = Math.min(len, (int) buffer.remaining());
    buffer.read(b, off, read);
    return read;
  }

  @Override
  public long skip(long n) throws IOException {
    int skipped = (int) Math.min(n, buffer.remaining());
    buffer.skip(skipped);
    return skipped;
  }

  @Override
  public int available() throws IOException {
    return (int) buffer.remaining();
  }

  @Override
  public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reset() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void close() throws IOException {
    buffer.close();
  }

}
