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

package io.atomix.primitives.map;

import io.atomix.primitives.PrimitiveException;

/**
 * Top level exception for ConsistentMap failures.
 */
@SuppressWarnings("serial")
public class ConsistentMapException extends PrimitiveException {
  public ConsistentMapException() {
  }

  public ConsistentMapException(String message) {
    super(message);
  }

  public ConsistentMapException(Throwable t) {
    super(t);
  }

  /**
   * ConsistentMap operation timeout.
   */
  public static class Timeout extends ConsistentMapException {
    public Timeout() {
      super();
    }

    public Timeout(String message) {
      super(message);
    }
  }

  /**
   * ConsistentMap update conflicts with an in flight transaction.
   */
  public static class ConcurrentModification extends ConsistentMapException {
    public ConcurrentModification() {
      super();
    }

    public ConcurrentModification(String message) {
      super(message);
    }
  }

  /**
   * ConsistentMap operation interrupted.
   */
  public static class Interrupted extends ConsistentMapException {
  }
}
