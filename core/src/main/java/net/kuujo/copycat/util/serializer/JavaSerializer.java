/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.util.serializer;

import net.kuujo.copycat.util.serializer.SerializationException;
import net.kuujo.copycat.util.serializer.Serializer;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Java serializer implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class JavaSerializer implements Serializer {
  private final ByteArrayOutputStream outputStream;
  private final ObjectOutputStream objectStream;

  public JavaSerializer() {
    try {
      this.outputStream = new ByteArrayOutputStream();
      this.objectStream = new ObjectOutputStream(outputStream);
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> T readObject(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    try (ObjectInputStream objectStream = new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), new ByteArrayInputStream(bytes))) {
      return (T) objectStream.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public synchronized ByteBuffer writeObject(Object object) {
    try {
      objectStream.writeObject(object);
      ByteBuffer output = ByteBuffer.wrap(outputStream.toByteArray());
      outputStream.reset();
      objectStream.reset();
      return output;
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

  /**
   * Object input stream that loads the class from the current context class loader.
   */
  private static class ClassLoaderObjectInputStream extends ObjectInputStream {
    private final ClassLoader cl;

    public ClassLoaderObjectInputStream(ClassLoader cl, InputStream in) throws IOException {
      super(in);
      this.cl = cl;
    }

    @Override
    public Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException, IOException {
      try {
        return cl.loadClass(desc.getName());
      } catch (Exception e) {
      }
      return super.resolveClass(desc);
    }

  }

}
