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
package net.kuujo.copycat.spi;

import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.AsyncCopycatContext;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.spi.protocol.AsyncProtocol;

/**
 * Copycat context factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncCopycatContextFactory {

  /**
   * Creates a Copycat context.
   *
   * @param stateMachine The Copycat state machine.
   * @param log The Copycat log.
   * @param cluster The Copycat cluster.
   * @param protocol The Copycat protocol.
   * @param config The Copycat configuration.
   * @return A new Copycat context.
   */
  <M extends Member> AsyncCopycatContext createContext(StateMachine stateMachine, Log log, Cluster<M> cluster, AsyncProtocol<M> protocol, CopycatConfig config);

}
