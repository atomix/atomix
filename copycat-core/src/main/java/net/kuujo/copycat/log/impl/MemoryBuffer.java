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
package net.kuujo.copycat.log.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import net.kuujo.copycat.log.Buffer;
import net.kuujo.copycat.log.LogException;
import net.kuujo.copycat.serializer.SerializationException;

/**
 * Memory-based byte buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemoryBuffer implements Buffer {
  private final ClassLoader cl = Thread.currentThread().getContextClassLoader();
  private ByteArrayOutputStream stream;
  private ByteBuffer buffer;

  MemoryBuffer() {
    this.stream = new ByteArrayOutputStream();
  }

  MemoryBuffer(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public boolean getBoolean() {
    return buffer.getShort() == 1;
  }

  @Override
  public boolean getBoolean(int offset) {
    return buffer.getShort(offset) == 1;
  }

  @Override
  public short getShort() {
    return buffer.getShort();
  }

  @Override
  public short getShort(int offset) {
    return buffer.getShort(offset);
  }

  @Override
  public int getInt() {
    return buffer.getInt();
  }

  @Override
  public int getInt(int offset) {
    return buffer.getInt(offset);
  }

  @Override
  public long getLong() {
    return buffer.getLong();
  }

  @Override
  public long getLong(int offset) {
    return buffer.getLong(offset);
  }

  @Override
  public double getDouble() {
    return buffer.getDouble();
  }

  @Override
  public double getDouble(int offset) {
    return buffer.getDouble(offset);
  }

  @Override
  public float getFloat() {
    return buffer.getFloat();
  }

  @Override
  public float getFloat(int offset) {
    return buffer.getFloat(offset);
  }

  @Override
  public char getChar() {
    return buffer.getChar();
  }

  @Override
  public char getChar(int offset) {
    return buffer.getChar(offset);
  }

  @Override
  public byte getByte() {
    return buffer.get();
  }

  @Override
  public byte getByte(int offset) {
    return buffer.get(offset);
  }

  @Override
  public byte[] getBytes(int length) {
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return bytes;
  }

  @Override
  public byte[] getBytes(int offset, int length) {
    byte[] bytes = new byte[length];
    buffer.get(bytes, offset, length);
    return bytes;
  }

  @Override
  public String getString(int length) {
    return new String(getBytes(length));
  }

  @Override
  public String getString(int offset, int length) {
    return new String(getBytes(offset, length));
  }

  @Override
  public <T extends Map<K, V>, K, V> T getMap(T map, Class<K> keyType, Class<V> valueType) {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return deserializeObject(bytes);
  }

  @Override
  public <T extends Map<K, V>, K, V> T getMap(int offset, T map, Class<K> keyType, Class<V> valueType) {
    buffer.position(offset);
    return getMap(map, keyType, valueType);
  }

  @Override
  public <T extends Collection<U>, U> T getCollection(T collection, Class<U> type) {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return deserializeObject(bytes);
  }

  @Override
  public <T extends Collection<U>, U> T getCollection(int offset, T collection, Class<U> type) {
    buffer.position(offset);
    return getCollection(collection, type);
  }

  @Override
  public Buffer setBoolean(int offset, boolean value) {
    stream.write(ByteBuffer.allocate(4).putInt(value ? 1 : 0).array(), offset, 4);
    return this;
  }

  @Override
  public Buffer setShort(int offset, short value) {
    stream.write(ByteBuffer.allocate(2).putShort(value).array(), offset, 2);
    return this;
  }

  @Override
  public Buffer setInt(int offset, int value) {
    stream.write(ByteBuffer.allocate(4).putInt(value).array(), offset, 4);
    return this;
  }

  @Override
  public Buffer setLong(int offset, long value) {
    stream.write(ByteBuffer.allocate(8).putLong(value).array(), offset, 8);
    return this;
  }

  @Override
  public Buffer setDouble(int offset, double value) {
    stream.write(ByteBuffer.allocate(8).putDouble(value).array(), offset, 8);
    return this;
  }

  @Override
  public Buffer setFloat(int offset, float value) {
    stream.write(ByteBuffer.allocate(4).putFloat(value).array(), offset, 4);
    return this;
  }

  @Override
  public Buffer setChar(int offset, char value) {
    stream.write(ByteBuffer.allocate(2).putChar(value).array(), offset, 2);
    return this;
  }

  @Override
  public Buffer setByte(int offset, byte value) {
    stream.write(new byte[]{value}, offset, 1);
    return this;
  }

  @Override
  public Buffer setBytes(int offset, byte[] value) {
    stream.write(value, offset, value.length);
    return this;
  }

  @Override
  public Buffer setString(int offset, String value) {
    return setBytes(offset, value.getBytes());
  }

  @Override
  public <K, V> Buffer setMap(int offset, Map<K, V> map) {
    byte[] bytes = serializeObject(map);
    stream.write(ByteBuffer.allocate(4).putInt(bytes.length).array(), offset, 4);
    stream.write(bytes, offset + 4, bytes.length);
    return this;
  }

  @Override
  public <T> Buffer setCollection(int offset, Collection<T> collection) {
    byte[] bytes = serializeObject(collection);
    stream.write(ByteBuffer.allocate(4).putInt(bytes.length).array(), offset, 4);
    stream.write(bytes, offset + 4, bytes.length);
    return this;
  }

  @Override
  public Buffer appendBoolean(boolean value) {
    try {
      stream.write(ByteBuffer.allocate(4).putInt(value ? 1 : 0).array());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Buffer appendShort(short value) {
    try {
      stream.write(ByteBuffer.allocate(2).putShort(value).array());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Buffer appendInt(int value) {
    try {
      stream.write(ByteBuffer.allocate(4).putInt(value).array());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Buffer appendLong(long value) {
    try {
      stream.write(ByteBuffer.allocate(8).putLong(value).array());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Buffer appendDouble(double value) {
    try {
      stream.write(ByteBuffer.allocate(8).putDouble(value).array());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Buffer appendFloat(float value) {
    try {
      stream.write(ByteBuffer.allocate(4).putFloat(value).array());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Buffer appendChar(char value) {
    try {
      stream.write(ByteBuffer.allocate(2).putChar(value).array());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Buffer appendByte(byte value) {
    stream.write(value);
    return this;
  }

  @Override
  public Buffer appendBytes(byte[] value) {
    try {
      stream.write(value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public Buffer appendString(String value) {
    return appendBytes(value.getBytes());
  }

  @Override
  public <K, V> Buffer appendMap(Map<K, V> map) {
    byte[] bytes = serializeObject(map);
    try {
      stream.write(ByteBuffer.allocate(4).putInt(bytes.length).array());
      stream.write(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public <T> Buffer appendCollection(Collection<T> collection) {
    byte[] bytes = serializeObject(collection);
    try {
      stream.write(ByteBuffer.allocate(4).putInt(bytes.length).array());
      stream.write(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  ByteBuffer toByteBuffer() {
    return stream != null ? ByteBuffer.wrap(stream.toByteArray()) : buffer;
  }

  /**
   * Serializes a serializeable object.
   */
  private byte[] serializeObject(Object object) {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ObjectOutputStream stream = null;
    try {
      stream = new ObjectOutputStream(byteStream);
      stream.writeObject(object);
    } catch (IOException e) {
      throw new LogException(e.getMessage());
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

  /**
   * Deserializes a serializeable object.
   */
  @SuppressWarnings("unchecked")
  private <T> T deserializeObject(byte[] bytes) {
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
