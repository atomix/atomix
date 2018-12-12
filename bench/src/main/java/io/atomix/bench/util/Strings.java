/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.bench.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * String utilities.
 */
public final class Strings {
  private static final char[] CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

  /**
   * Creates a deterministic array of strings to write to the cluster.
   *
   * @param length the string lengths
   * @param count  the string count
   * @return a deterministic array of strings
   */
  public static String[] randomStrings(int length, int count) {
    Random random = new Random(length);
    List<String> stringsList = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      stringsList.add(randomString(length, random));
    }
    return stringsList.toArray(new String[0]);
  }

  /**
   * Creates a deterministic string based on the given seed.
   *
   * @param length the seed from which to create the string
   * @param random the random object from which to create the string characters
   * @return the string
   */
  public static String randomString(int length, Random random) {
    char[] buffer = new char[length];
    for (int i = 0; i < length; i++) {
      buffer[i] = CHARS[random.nextInt(CHARS.length)];
    }
    return new String(buffer);
  }

  private Strings() {
  }
}
