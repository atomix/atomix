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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.spi.CopycatContextFactory;

/**
 * Default Copycat context factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycatContextFactory implements CopycatContextFactory {

  @Override
  public CopycatContext createContext(StateMachine stateMachine, Log log, Cluster<?> cluster, CopycatConfig config) {
    return new DefaultCopycatContext(stateMachine, log, cluster, config);
  }

}
