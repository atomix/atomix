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

import java.util.Collection;
import java.util.Map;

import net.kuujo.copycat.log.Buffer;
import net.openhft.chronicle.ExcerptCommon;

/**
 * Java chronicle buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PersistentBuffer implements Buffer {
  private final ExcerptCommon excerpt;

  PersistentBuffer(ExcerptCommon excerpt) {
    this.excerpt = excerpt;
  }

  @Override
  public boolean getBoolean() {
    return excerpt.readBoolean();
  }

  @Override
  public boolean getBoolean(int offset) {
    return excerpt.readBoolean(offset);
  }

  @Override
  public short getShort() {
    return excerpt.readShort();
  }

  @Override
  public short getShort(int offset) {
    return excerpt.readShort(offset);
  }

  @Override
  public int getInt() {
    return excerpt.readInt();
  }

  @Override
  public int getInt(int offset) {
    return excerpt.readInt(offset);
  }

  @Override
  public long getLong() {
    return excerpt.readLong();
  }

  @Override
  public long getLong(int offset) {
    return excerpt.readLong(offset);
  }

  @Override
  public double getDouble() {
    return excerpt.readDouble();
  }

  @Override
  public double getDouble(int offset) {
    return excerpt.readDouble(offset);
  }

  @Override
  public float getFloat() {
    return excerpt.readFloat();
  }

  @Override
  public float getFloat(int offset) {
    return excerpt.readFloat(offset);
  }

  @Override
  public char getChar() {
    return excerpt.readChar();
  }

  @Override
  public char getChar(int offset) {
    return excerpt.readChar(offset);
  }

  @Override
  public byte getByte() {
    return excerpt.readByte();
  }

  @Override
  public byte getByte(int offset) {
    return excerpt.readByte(offset);
  }

  @Override
  public byte[] getBytes(int length) {
    byte[] bytes = new byte[length];
    excerpt.read(bytes);
    return bytes;
  }

  @Override
  public byte[] getBytes(int offset, int length) {
    byte[] bytes = new byte[length];
    excerpt.read(bytes, offset, length);
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
    excerpt.readMap(map, keyType, valueType);
    return map;
  }

  @Override
  public <T extends Map<K, V>, K, V> T getMap(int offset, T map, Class<K> keyType, Class<V> valueType) {
    excerpt.position(offset);
    excerpt.readMap(map, keyType, valueType);
    return map;
  }

  @Override
  public <T extends Collection<U>, U> T getCollection(T collection, Class<U> type) {
    excerpt.readList(collection, type);
    return collection;
  }

  @Override
  public <T extends Collection<U>, U> T getCollection(int offset, T collection, Class<U> type) {
    excerpt.position(offset);
    excerpt.readList(collection, type);
    return collection;
  }

  @Override
  public Buffer setBoolean(int offset, boolean value) {
    excerpt.writeBoolean(offset, value);
    return this;
  }

  @Override
  public Buffer setShort(int offset, short value) {
    excerpt.writeShort(offset, value);
    return this;
  }

  @Override
  public Buffer setInt(int offset, int value) {
    excerpt.writeInt(offset, value);
    return this;
  }

  @Override
  public Buffer setLong(int offset, long value) {
    excerpt.writeLong(offset, value);
    return this;
  }

  @Override
  public Buffer setDouble(int offset, double value) {
    excerpt.writeDouble(offset, value);
    return this;
  }

  @Override
  public Buffer setFloat(int offset, float value) {
    excerpt.writeFloat(offset, value);
    return this;
  }

  @Override
  public Buffer setChar(int offset, char value) {
    excerpt.writeChar(offset, value);
    return this;
  }

  @Override
  public Buffer setByte(int offset, byte value) {
    excerpt.writeByte(offset, value);
    return this;
  }

  @Override
  public Buffer setBytes(int offset, byte[] value) {
    excerpt.write(offset, value);
    return this;
  }

  @Override
  public Buffer setString(int offset, String value) {
    excerpt.write(offset, value.getBytes());
    return this;
  }

  @Override
  public <K, V> Buffer setMap(int offset, Map<K, V> map) {
    excerpt.position(offset);
    excerpt.writeMap(map);
    return this;
  }

  @Override
  public <T> Buffer setCollection(int offset, Collection<T> collection) {
    excerpt.position(offset);
    excerpt.writeList(collection);
    return this;
  }

  @Override
  public Buffer appendBoolean(boolean value) {
    excerpt.writeBoolean(value);
    return this;
  }

  @Override
  public Buffer appendShort(short value) {
    excerpt.writeShort(value);
    return this;
  }

  @Override
  public Buffer appendInt(int value) {
    excerpt.writeInt(value);
    return this;
  }

  @Override
  public Buffer appendLong(long value) {
    excerpt.writeLong(value);
    return this;
  }

  @Override
  public Buffer appendDouble(double value) {
    excerpt.writeDouble(value);
    return this;
  }

  @Override
  public Buffer appendFloat(float value) {
    excerpt.writeFloat(value);
    return this;
  }

  @Override
  public Buffer appendChar(char value) {
    excerpt.writeChar(value);
    return this;
  }

  @Override
  public Buffer appendByte(byte value) {
    excerpt.writeByte(value);
    return this;
  }

  @Override
  public Buffer appendBytes(byte[] bytes) {
    excerpt.write(bytes);
    return this;
  }

  @Override
  public Buffer appendString(String value) {
    excerpt.writeBytes(value);
    return this;
  }

  @Override
  public <K, V> Buffer appendMap(Map<K, V> map) {
    excerpt.writeMap(map);
    return this;
  }

  @Override
  public <T> Buffer appendCollection(Collection<T> collection) {
    excerpt.writeList(collection);
    return this;
  }

}
