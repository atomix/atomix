/*
 * Copyright 2015 the original author or authors.
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

/**
 * Context for unregistering a registered listener.
 * <p>
 * The listener context represents a registered listener. The context is normally returned when a {@link Listener} is
 * registered and can be used to unregister the listener via {@link #close()}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ListenerContext<T> extends Listener<T>, AutoCloseable {

  /**
   * Closes the listener.
   * <p>
   * When the listener is closed, the listener will be unregistered and will no longer receive events for which it was
   * listening.
   */
  @Override
  void close();

}
