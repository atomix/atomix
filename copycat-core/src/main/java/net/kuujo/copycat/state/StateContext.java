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

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.EventProvider;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.Log;

/**
 * Container for replica state information.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateContext extends EventProvider<StateListener> {

  /**
   * Returns the copycat context.
   *
   * @return The copycat context.
   */
  CopyCatContext context();

  /**
   * Returns the internal state cluster configuration.
   *
   * @return The internal state cluster configuration.
   */
  ClusterConfig cluster();

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  Log log();

  /**
   * Returns a boolean indicating whether the state is leader.
   *
   * @return Indicates whether the current state is leader.
   */
  boolean isLeader();

}
