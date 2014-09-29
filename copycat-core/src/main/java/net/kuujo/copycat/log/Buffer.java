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
package net.kuujo.copycat.log;

import java.util.Collection;
import java.util.Map;

/**
 * Log entry byte buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Buffer {

  /**
   * Returns a boolean value at the current buffer position.
   *
   * @return A boolean value at the current buffer position.
   */
  boolean getBoolean();

  /**
   * Returns a boolean value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @return A boolean value at the given offset.
   */
  boolean getBoolean(int offset);

  /**
   * Returns a short value at the current buffer position.
   *
   * @return A short value at the current buffer position.
   */
  short getShort();

  /**
   * Returns a short value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @return A short value at the given offset.
   */
  short getShort(int offset);

  /**
   * Returns a integer value at the current buffer position.
   *
   * @return A integer value at the current buffer position.
   */
  int getInt();

  /**
   * Returns a integer value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @return A integer value at the given offset.
   */
  int getInt(int offset);

  /**
   * Returns a long value at the current buffer position.
   *
   * @return A long value at the current buffer position.
   */
  long getLong();

  /**
   * Returns a long value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @return A long value at the given offset.
   */
  long getLong(int offset);

  /**
   * Returns a double value at the current buffer position.
   *
   * @return A double value at the current buffer position.
   */
  double getDouble();

  /**
   * Returns a double value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @return A double value at the given offset.
   */
  double getDouble(int offset);

  /**
   * Returns a float value at the current buffer position.
   *
   * @return A float value at the current buffer position.
   */
  float getFloat();

  /**
   * Returns a float value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @return A float value at the given offset.
   */
  float getFloat(int offset);

  /**
   * Returns a character value at the current buffer position.
   *
   * @return A character value at the current buffer position.
   */
  char getChar();

  /**
   * Returns a character value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @return A character value at the given offset.
   */
  char getChar(int offset);

  /**
   * Returns a byte value at the current buffer position.
   *
   * @return A byte value at the current buffer position.
   */
  byte getByte();

  /**
   * Returns a byte value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @return A byte value at the given offset.
   */
  byte getByte(int offset);

  /**
   * Returns a byte array value at the current buffer position.
   *
   * @return A byte array value at the current buffer position.
   */
  byte[] getBytes(int length);

  /**
   * Returns a byte array value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @return A byte array value at the given offset.
   */
  byte[] getBytes(int offset, int length);

  /**
   * Returns a string value at the current buffer position.
   *
   * @return A string value at the current buffer position.
   */
  String getString(int length);

  /**
   * Returns a string value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @return A string value at the given offset.
   */
  String getString(int offset, int length);

  /**
   * Returns a map value at the current buffer position.
   *
   * @param map A map instance to populate.
   * @param keyType The map key type.
   * @param valueType The map value type.
   * @return The populated map instance.
   */
  <T extends Map<K, V>, K, V> T getMap(T map, Class<K> keyType, Class<V> valueType);

  /**
   * Returns a map value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @param map A map instance to populate.
   * @param keyType The map key type.
   * @param valueType The map value type.
   * @return The populated map instance.
   */
  <T extends Map<K, V>, K, V> T getMap(int offset, T map, Class<K> keyType, Class<V> valueType);

  /**
   * Returns a collection value at the current buffer position.
   *
   * @param collection A collection instance to populate.
   * @param type The collection type.
   * @return The populated collection instance.
   */
  <T extends Collection<U>, U> T getCollection(T collection, Class<U> type);

  /**
   * Returns a collection value at the given offset.
   *
   * @param offset The offset at which to read the value.
   * @param collection A collection instance to populate.
   * @param type The collection type.
   * @return The populated collection instance.
   */
  <T extends Collection<U>, U> T getCollection(int offset, T collection, Class<U> type);

  /**
   * Sets a boolean at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param value The value to set.
   * @return The buffer instance.
   */
  Buffer setBoolean(int offset, boolean value);

  /**
   * Sets a short at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param value The value to set.
   * @return The buffer instance.
   */
  Buffer setShort(int offset, short value);

  /**
   * Sets a integer at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param value The value to set.
   * @return The buffer instance.
   */
  Buffer setInt(int offset, int value);

  /**
   * Sets a long at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param value The value to set.
   * @return The buffer instance.
   */
  Buffer setLong(int offset, long value);

  /**
   * Sets a double at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param value The value to set.
   * @return The buffer instance.
   */
  Buffer setDouble(int offset, double value);

  /**
   * Sets a float at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param value The value to set.
   * @return The buffer instance.
   */
  Buffer setFloat(int offset, float value);

  /**
   * Sets a character at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param value The value to set.
   * @return The buffer instance.
   */
  Buffer setChar(int offset, char value);

  /**
   * Sets a byte at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param value The value to set.
   * @return The buffer instance.
   */
  Buffer setByte(int offset, byte value);

  /**
   * Sets a byte array at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param value The value to set.
   * @return The buffer instance.
   */
  Buffer setBytes(int offset, byte[] value);

  /**
   * Sets a string at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param value The value to set.
   * @return The buffer instance.
   */
  Buffer setString(int offset, String value);

  /**
   * Sets a map at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param map The value to set.
   * @return The buffer instance.
   */
  <K, V> Buffer setMap(int offset, Map<K, V> map);

  /**
   * Sets a collection at the given offset.
   *
   * @param offset The offset at which to set the value.
   * @param collection The value to set.
   * @return The buffer instance.
   */
  <T> Buffer setCollection(int offset, Collection<T> collection);

  /**
   * Appends a boolean value to the buffer.
   *
   * @param value The value to append.
   * @return The buffer instance.
   */
  Buffer appendBoolean(boolean value);

  /**
   * Appends a short value to the buffer.
   *
   * @param value The value to append.
   * @return The buffer instance.
   */
  Buffer appendShort(short value);

  /**
   * Appends a integer value to the buffer.
   *
   * @param value The value to append.
   * @return The buffer instance.
   */
  Buffer appendInt(int value);

  /**
   * Appends a long value to the buffer.
   *
   * @param value The value to append.
   * @return The buffer instance.
   */
  Buffer appendLong(long value);

  /**
   * Appends a double value to the buffer.
   *
   * @param value The value to append.
   * @return The buffer instance.
   */
  Buffer appendDouble(double value);

  /**
   * Appends a float value to the buffer.
   *
   * @param value The value to append.
   * @return The buffer instance.
   */
  Buffer appendFloat(float value);

  /**
   * Appends a character value to the buffer.
   *
   * @param value The value to append.
   * @return The buffer instance.
   */
  Buffer appendChar(char value);

  /**
   * Appends a byte value to the buffer.
   *
   * @param value The value to append.
   * @return The buffer instance.
   */
  Buffer appendByte(byte value);

  /**
   * Appends a byte array value to the buffer.
   *
   * @param value The value to append.
   * @return The buffer instance.
   */
  Buffer appendBytes(byte[] value);

  /**
   * Appends a string value to the buffer.
   *
   * @param value The value to append.
   * @return The buffer instance.
   */
  Buffer appendString(String value);

  /**
   * Appends a map value to the buffer.
   *
   * @param map The value to append.
   * @return The buffer instance.
   */
  <K, V> Buffer appendMap(Map<K, V> map);

  /**
   * Appends a collection value to the buffer.
   *
   * @param collection The value to append.
   * @return The buffer instance.
   */
  <T> Buffer appendCollection(Collection<T> collection);

}
