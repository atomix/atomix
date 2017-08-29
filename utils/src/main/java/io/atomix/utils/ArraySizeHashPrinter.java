/*
 * Copyright 2014-present Open Networking Foundation
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

package io.atomix.utils;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Helper to print Object[] length and hashCode.
 */
public final class ArraySizeHashPrinter {

  /**
   * Returns ByteArraySizeHashPrinter wrapping given short[].
   *
   * @param array arrays to wrap around
   * @return ObjectArraySizeHashPrinter
   */
  public static ArraySizeHashPrinter of(byte[] array) {
    return new ArraySizeHashPrinter(toObjectArray(array), byte[].class);
  }

  /**
   * Returns ByteArraySizeHashPrinter wrapping given short[].
   *
   * @param array arrays to wrap around
   * @return ObjectArraySizeHashPrinter
   */
  public static ArraySizeHashPrinter of(short[] array) {
    return new ArraySizeHashPrinter(toObjectArray(array), short[].class);
  }

  /**
   * Returns ByteArraySizeHashPrinter wrapping given int[].
   *
   * @param array arrays to wrap around
   * @return ObjectArraySizeHashPrinter
   */
  public static ArraySizeHashPrinter of(int[] array) {
    return new ArraySizeHashPrinter(toObjectArray(array), int[].class);
  }

  /**
   * Returns ByteArraySizeHashPrinter wrapping given long[].
   *
   * @param array arrays to wrap around
   * @return ObjectArraySizeHashPrinter
   */
  public static ArraySizeHashPrinter of(long[] array) {
    return new ArraySizeHashPrinter(toObjectArray(array), long[].class);
  }

  /**
   * Returns ByteArraySizeHashPrinter wrapping given float[].
   *
   * @param array arrays to wrap around
   * @return ObjectArraySizeHashPrinter
   */
  public static ArraySizeHashPrinter of(float[] array) {
    return new ArraySizeHashPrinter(toObjectArray(array), float[].class);
  }

  /**
   * Returns ByteArraySizeHashPrinter wrapping given double[].
   *
   * @param array arrays to wrap around
   * @return ObjectArraySizeHashPrinter
   */
  public static ArraySizeHashPrinter of(double[] array) {
    return new ArraySizeHashPrinter(toObjectArray(array), double[].class);
  }

  /**
   * Returns ByteArraySizeHashPrinter wrapping given boolean[].
   *
   * @param array arrays to wrap around
   * @return ObjectArraySizeHashPrinter
   */
  public static ArraySizeHashPrinter of(boolean[] array) {
    return new ArraySizeHashPrinter(toObjectArray(array), boolean[].class);
  }

  /**
   * Returns ByteArraySizeHashPrinter wrapping given Object[].
   *
   * @param array arrays to wrap around
   * @return ObjectArraySizeHashPrinter
   */
  public static ArraySizeHashPrinter of(Object[] array) {
    return new ArraySizeHashPrinter(array, Object[].class);
  }

  private static Object[] toObjectArray(Object val) {
    if (val instanceof Object[])
      return (Object[]) val;
    int length = Array.getLength(val);
    Object[] outputArray = new Object[length];
    for (int i = 0; i < length; ++i) {
      outputArray[i] = Array.get(val, i);
    }
    return outputArray;
  }

  private final Object[] array;
  private final Class<?> type;

  public ArraySizeHashPrinter(Object[] array, Class<?> type) {
    this.array = array;
    this.type = type;
  }

  @Override
  public String toString() {
    ToStringHelper helper = MoreObjects.toStringHelper(type);
    if (array != null) {
      helper.add("length", array.length)
          .add("hash", Arrays.hashCode(array));
    } else {
      helper.addValue(array);
    }
    return helper.toString();
  }
}
