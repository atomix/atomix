/*
 * Copyright 2017-present Open Networking Foundation
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

/**
 * Atomix I/O exception.
 */
public class AtomixIOException extends AtomixRuntimeException {
  public AtomixIOException() {
  }

  public AtomixIOException(String message) {
    super(message);
  }

  public AtomixIOException(String message, Object... args) {
    super(message, args);
  }

  public AtomixIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public AtomixIOException(Throwable cause) {
    super(cause);
  }
}
