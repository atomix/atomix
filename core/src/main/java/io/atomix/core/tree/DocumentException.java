/*
 * Copyright 2016-present Open Networking Foundation
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

package io.atomix.core.tree;

/**
 * Exceptions for use by the {@code DocumentTree} and {@code DocumentPath}.
 */
public class DocumentException extends RuntimeException {
  public DocumentException() {
    super();
  }

  public DocumentException(String message) {
    super(message);
  }

  public DocumentException(String message, Throwable cause) {
    super(message, cause);
  }

  public DocumentException(Throwable cause) {
    super(cause);
  }

  /**
   * DocumentTree operation timeout.
   */
  public static class Timeout extends DocumentException {
    public Timeout() {
    }

    public Timeout(String message) {
      super(message);
    }

    public Timeout(String message, Throwable cause) {
      super(message, cause);
    }

    public Timeout(Throwable cause) {
      super(cause);
    }
  }

  /**
   * DocumentTree operation interrupted.
   */
  public static class Interrupted extends DocumentException {
  }
}
