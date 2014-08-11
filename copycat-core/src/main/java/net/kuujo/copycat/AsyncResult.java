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
package net.kuujo.copycat;

/**
 * Asynchronous result.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncResult<T> {
  private final boolean succeeded;
  private final T value;
  private final Throwable error;

  public AsyncResult(T value) {
    this.succeeded = true;
    this.value = value;
    this.error = null;
  }

  public AsyncResult(Throwable t) {
    this.succeeded = false;
    this.value = null;
    this.error = t;
  }

  /**
   * Returns a boolean indicating whether the result succeeded.
   *
   * @return Indicates whether the result succeeded.
   */
  public boolean succeeded() {
    return succeeded;
  }

  /**
   * Returns the asynchronous result value.
   *
   * @return The asynchronous result value.
   */
  public T value() {
    return value;
  }

  /**
   * Returns a boolean indicating whether the result failed.
   *
   * @return Indicates whether the result failed.
   */
  public boolean failed() {
    return !succeeded;
  }

  /**
   * Returns the result error.
   *
   * @return The result error.
   */
  public Throwable cause() {
    return error;
  }

}
