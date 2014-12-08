/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ClusterContext;
import net.kuujo.copycat.internal.DefaultCopycatContext;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.RaftProtocol;
import net.kuujo.copycat.spi.ExecutionContext;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RaftContext extends Managed, RaftProtocol {

  /**
   * Creates a new Copycat context.
   *
   * @param log The Copycat log.
   * @return The Copycat context.
   */
  static RaftContext create(Log log, ClusterConfig cluster) {
    return new DefaultCopycatContext(log, new ClusterContext(cluster), ExecutionContext.create());
  }

  /**
   * Returns the current context state.
   *
   * @return The current context state.
   */
  CopycatState state();

  /**
   * Returns the Copycat log.
   *
   * @return The context log.
   */
  Log log();

  /**
   * Returns the Copycat cluster context.
   *
   * @return The Copycat cluster context.
   */
  ClusterContext cluster();

  /**
   * Returns the Copycat context executor.
   *
   * @return The execution context.
   */
  ExecutionContext executor();

}
