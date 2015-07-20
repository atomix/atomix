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
package net.kuujo.copycat;

/**
 * Event listener.
 * <p>
 * This is a simple functional event listener interface used to listen for events from a variety of objects. When a listener
 * is registered, a {@link net.kuujo.copycat.ListenerContext} is typically returned. The context can be used to unregister
 * the listener at any time via {@link ListenerContext#close()}.
 * <p>
 * In all cases Copycat will ensure that a registered listener will <em>always</em> be {@link #accept(Object) invoked}
 * on the same {@link net.kuujo.copycat.util.concurrent.CopycatThread Copycat thread}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@FunctionalInterface
public interface Listener<T> {

  /**
   * Calls the listener.
   * <p>
   * The listener will always be called on the same {@link net.kuujo.copycat.util.concurrent.CopycatThread Copycat thread}.
   *
   * @param event The event that occurred.
   */
  void accept(T event);

}
