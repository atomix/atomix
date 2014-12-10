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
package net.kuujo.copycat.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Tuple implements Serializable {
  private final List<Object> tuple;

  public Tuple() {
    this.tuple = new ArrayList<>(10);
  }

  public Tuple(Object... values) {
    this.tuple = new ArrayList<>(Arrays.asList(values));
  }

  public Tuple addString(String value) {
    tuple.add(value);
    return this;
  }

  public Tuple addShort(short value) {
    tuple.add(value);
    return this;
  }

  public Tuple addInteger(int value) {
    tuple.add(value);
    return this;
  }

  public Tuple addLong(long value) {
    tuple.add(value);
    return this;
  }

  public Tuple addFloat(float value) {
    tuple.add(value);
    return this;
  }

  public Tuple addDouble(double value) {
    tuple.add(value);
    return this;
  }

  public Tuple addBoolean(boolean value) {
    tuple.add(value);
    return this;
  }

  public Tuple addValue(Object value) {
    tuple.add(value);
    return this;
  }

  public String getString(int index) {
    Object value = tuple.get(index);
    return value != null ? value.toString() : null;
  }

  public Short getShort(int index) {
    Object value = tuple.get(index);
    return value != null ? (Short) value : null;
  }

  public Integer getInteger(int index) {
    Object value = tuple.get(index);
    return value != null ? (Integer) value : null;
  }

  public Long getLong(int index) {
    Object value = tuple.get(index);
    return value != null ? (Long) value : null;
  }

  public Float getFloat(int index) {
    Object value = tuple.get(index);
    return value != null ? (Float) value : null;
  }

  public Double getDouble(int index) {
    Object value = tuple.get(index);
    return value != null ? (Double) value : null;
  }

  public Boolean getBoolean(int index) {
    Object value = tuple.get(index);
    return value != null ? (Boolean) value : null;
  }

  @SuppressWarnings("unchecked")
  public <T> T getValue(int index) {
    return (T) tuple.get(index);
  }

  public int size() {
    return tuple.size();
  }

  public boolean isEmpty() {
    return tuple.isEmpty();
  }

  public void clear() {
    tuple.clear();
  }

}
