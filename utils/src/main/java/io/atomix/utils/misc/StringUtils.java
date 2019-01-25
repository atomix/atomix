/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.utils.misc;

import java.util.ArrayList;
import java.util.List;

/**
 * Collection of various helper methods to manipulate strings.
 */
public final class StringUtils {

  private StringUtils() {
  }

  /**
   * Splits the input string with the given regex and filters empty strings.
   *
   * @param input the string to split.
   * @return the array of strings computed by splitting this string
   */
  public static String[] split(String input, String regex) {
    if (input == null) {
      return null;
    }
    String[] arr = input.split(regex);
    List<String> results = new ArrayList<>(arr.length);
    for (String a : arr) {
      if (!a.trim().isEmpty()) {
        results.add(a);
      }
    }
    return results.toArray(new String[0]);
  }
}
