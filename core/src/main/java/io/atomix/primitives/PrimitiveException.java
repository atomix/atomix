/*
 * Copyright 2015-present Open Networking Foundation
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

package io.atomix.primitives;

import io.atomix.utils.AtomixRuntimeException;

/**
 * Top level exception for Store failures.
 */
@SuppressWarnings("serial")
public class PrimitiveException extends AtomixRuntimeException {
  public PrimitiveException() {
  }

  public PrimitiveException(String message) {
    super(message);
  }

  public PrimitiveException(Throwable t) {
    super(t);
  }

  /**
   * Store is temporarily unavailable.
   */
  public static class Unavailable extends PrimitiveException {
  }

  /**
   * Store operation timeout.
   */
  public static class Timeout extends PrimitiveException {
  }

  /**
   * Store update conflicts with an in flight transaction.
   */
  public static class ConcurrentModification extends PrimitiveException {
  }

  /**
   * Store operation interrupted.
   */
  public static class Interrupted extends PrimitiveException {
  }
}
