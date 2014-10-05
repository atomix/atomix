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

import java.util.function.Predicate;

/**
 * Argument validation helpers.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Args {

  /**
   * Validates that a value is null.
   *
   * @param value The value to validate.
   */
  public static <T> T checkNull(T value) {
    if (value != null) {
      throw new NullPointerException();
    }
    return value;
  }

  /**
   * Validates that a value is null.
   *
   * @param value The value to validate.
   * @param message The exception message.
   * @param args A list of message string formatting arguments.
   */
  public static <T> T checkNull(T value, String message, Object... args) {
    if (value != null) {
      throw new NullPointerException(String.format(message, args));
    }
    return value;
  }

  /**
   * Validates that a valid is null.
   *
   * @param value The value to validate.
   * @param callback A callback to call if the value is not null.
   */
  public static <T> T checkNull(T value, Runnable callback) {
    if (value != null) {
      callback.run();
    }
    return value;
  }

  /**
   * Validates that a value is not null.
   *
   * @param value The value to validate.
   */
  public static <T> T checkNotNull(T value) {
    if (value == null) {
      throw new NullPointerException();
    }
    return value;
  }

  /**
   * Validates that a value is not null.
   *
   * @param value The value to validate.
   * @param message The exception message.
   * @param args A list of message string formatting arguments.
   */
  public static <T> T checkNotNull(T value, String message, Object... args) {
    if (value == null) {
      throw new NullPointerException(String.format(message, args));
    }
    return value;
  }

  /**
   * Validates that a valid is not null.
   *
   * @param value The value to validate.
   * @param callback A callback to call if the value is null.
   */
  public static <T> T checkNotNull(T value, Runnable callback) {
    if (value == null) {
      callback.run();
    }
    return value;
  }

  /**
   * Validates that a value meets a predicate.
   *
   * @param value The value to validate.
   * @param predicate The predicate with which to check the value.
   */
  public static <T> T checkValue(T value, Predicate<T> predicate) {
    if (!predicate.test(value)) {
      throw new IllegalArgumentException();
    }
    return value;
  }

  /**
   * Validates that a value meets a predicate.
   *
   * @param value The value to validate.
   * @param predicate The predicate with which to check the value.
   * @param message The failure exception message.
   * @param args A list of message string formatting arguments.
   */
  public static <T> T checkValue(T value, Predicate<T> predicate, String message, Object... args) {
    if (!predicate.test(value)) {
      throw new IllegalArgumentException(String.format(message, args));
    }
    return value;
  }

}
