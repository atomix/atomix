/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.util;

/**
 * Argument assertions.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class Assert {
  private Assert() {}

  /**
   * Validates that a value is not null.
   *
   * @param value The value to validate.
   * @throws NullPointerException if {@code value} is null
   */
  public static <T> T isNotNull(T value, String parameterName) {
    if (value == null) {
      throw new NullPointerException(String.format("%s cannot be null", parameterName));
    }
    return value;
  }

  /**
   * Validates that a value is null.
   *
   * @param value The value to validate.
   * @param message The exception message.
   * @param args A list of message string formatting arguments.
   * @throws NullPointerException if {@code value} is null
   */
  public static <T> T isNull(T value, String message, Object... args) {
    if (value != null) {
      throw new NullPointerException(String.format(message, args));
    }
    return value;
  }

  /**
   * Validates that a state applies.
   *
   * @param state The state to assert.
   * @param message The failure exception message.
   * @param args A list of message string formatting arguments.
   * @throws IllegalStateException if {@code state} is not true
   */
  public static void state(boolean state, String message, Object... args) {
    if (!state) {
      throw new IllegalStateException(String.format(message, args));
    }
  }

  /**
   * Validates that a condition applies.
   *
   * @param value The resulting value to passthrough
   * @param condition The condition to assert.
   * @param message The failure exception message.
   * @param args A list of message string formatting arguments.
   * @throws IllegalArgumentException if {@code condition} is not true
   */
  public static <T> T arg(T value, boolean condition, String message, Object... args) {
    if (!condition) {
      throw new IllegalArgumentException(String.format(message, args));
    }
    return value;
  }
}
