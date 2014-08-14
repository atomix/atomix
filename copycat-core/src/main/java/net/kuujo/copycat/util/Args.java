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
  public static void checkNull(Object value) {
    if (value != null) {
      throw new NullPointerException();
    }
  }

  /**
   * Validates that a value is null.
   *
   * @param value The value to validate.
   * @param message The exception message.
   */
  public static void checkNull(Object value, String message) {
    if (value != null) {
      throw new NullPointerException(message);
    }
  }

  /**
   * Validates that a valid is null.
   *
   * @param value The value to validate.
   * @param callback A callback to call if the value is not null.
   */
  public static void checkNull(Object value, Runnable callback) {
    if (value != null) {
      callback.run();
    }
  }

  /**
   * Validates that a value is not null.
   *
   * @param value The value to validate.
   */
  public static void checkNotNull(Object value) {
    if (value == null) {
      throw new NullPointerException();
    }
  }

  /**
   * Validates that a value is not null.
   *
   * @param value The value to validate.
   * @param message The exception message.
   */
  public static void checkNotNull(Object value, String message) {
    if (value == null) {
      throw new NullPointerException(message);
    }
  }

  /**
   * Validates that a valid is not null.
   *
   * @param value The value to validate.
   * @param callback A callback to call if the value is null.
   */
  public static void checkNotNull(Object value, Runnable callback) {
    if (value == null) {
      callback.run();
    }
  }

  /**
   * Validates that a value meets a predicate.
   *
   * @param value The value to validate.
   * @param predicate The predicate with which to check the value.
   */
  public static <T> void checkValue(T value, Predicate<T> predicate) {
    if (!predicate.test(value)) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Validates that a value meets a predicate.
   *
   * @param value The value to validate.
   * @param message The failure exception message.
   * @param predicate The predicate with which to check the value.
   */
  public static <T> void checkValue(T value, String message, Predicate<T> predicate) {
    if (!predicate.test(value)) {
      throw new IllegalArgumentException(message);
    }
  }

}
