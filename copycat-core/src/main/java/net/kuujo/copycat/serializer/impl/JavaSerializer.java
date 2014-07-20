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
package net.kuujo.copycat.serializer.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;

import net.kuujo.copycat.serializer.SerializationException;
import net.kuujo.copycat.serializer.Serializer;

/**
 * Java serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class JavaSerializer implements Serializer {
  private final ClassLoader cl;

  public JavaSerializer(ClassLoader cl) {
    this.cl = cl;
  }

  @Override
  public byte[] writeValue(Object value) {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ObjectOutputStream stream = null;
    try {
      stream = new ObjectOutputStream(byteStream);
      stream.writeObject(value);
    } catch (IOException e) {
      throw new SerializationException(e.getMessage());
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
        }
      }
    }
    return byteStream.toByteArray();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T readValue(byte[] bytes, Class<T> type) {
    ObjectInputStream stream = null;
    try {
      stream = new ClassLoaderObjectInputStream(cl, new ByteArrayInputStream(bytes));
      return (T) stream.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new SerializationException(e.getMessage());
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          throw new SerializationException(e.getMessage());
        }
      }
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
