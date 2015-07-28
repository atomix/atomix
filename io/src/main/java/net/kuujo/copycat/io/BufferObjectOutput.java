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
import java.io.ObjectOutput;

/**
 * Buffer object output wrapper.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferObjectOutput extends BufferDataOutput implements ObjectOutput {
  private final Serializer serializer;

  public BufferObjectOutput(BufferOutput buffer, Serializer serializer) {
    super(buffer);
    if (serializer == null)
      throw new NullPointerException("serializer cannot be null");
    this.serializer = serializer;
  }

  @Override
  public void writeObject(Object obj) throws IOException {
    serializer.writeObject(obj, buffer);
  }

  @Override
  public void flush() throws IOException {
    buffer.flush();
  }

  @Override
  public void close() throws IOException {
    buffer.close();
  }

}
