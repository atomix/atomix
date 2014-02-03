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
package net.kuujo.copycat.replication.state;

import java.util.ArrayDeque;
import java.util.Queue;

import org.vertx.java.core.Handler;

/**
 * A state lock.
 * 
 * @author Jordan Halterman
 */
class StateLock {
  private final Queue<Handler<Void>> handlers = new ArrayDeque<>();
  private boolean locked;

  /**
   * Acquires the lock.
   * 
   * @param handler A handler to be called once the lock is acquired.
   */
  public void acquire(Handler<Void> handler) {
    if (!locked) {
      locked = true;
      handler.handle((Void) null);
    }
    else {
      handlers.add(handler);
    }
  }

  /**
   * Releases the lock.
   */
  public void release() {
    if (!handlers.isEmpty()) {
      handlers.poll().handle((Void) null);
    }
    else {
      locked = false;
    }
  }

  /**
   * Returns a boolean indicating whether the lock is currently locked.
   */
  public boolean locked() {
    return locked;
  }

}
