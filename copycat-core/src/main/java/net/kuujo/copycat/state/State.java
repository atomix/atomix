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
package net.kuujo.copycat.state;

import net.kuujo.copycat.protocol.ProtocolHandler;

/**
 * Replica state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface State<T extends StateContext> extends ProtocolHandler {

  /**
   * State type.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public static enum Type {
    NONE,
    FOLLOWER,
    CANDIDATE,
    LEADER
  }

  /**
   * Returns the state type.
   *
   * @return The state type.
   */
  Type type();

  /**
   * Initializes the state.
   *
   * @param context The state context.
   */
  void init(T context);

  /**
   * Destroys the state.
   */
  void destroy();

}
