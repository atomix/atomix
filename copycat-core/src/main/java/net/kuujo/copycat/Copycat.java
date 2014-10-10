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

import net.kuujo.copycat.internal.SyncCopycatImpl;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.spi.protocol.Protocol;

/**
 * Copycat service.
 * <p>
 *
 * This is the primary type for implementing full remote services on top of Copycat. A
 * {@code Copycat} instance consists of a {@link net.kuujo.copycat.CopycatContext} which controls
 * logging and replication and a {@link net.kuujo.copycat.spi.service.Service} which exposes an
 * endpoint through which commands can be submitted to the Copycat cluster.
 * <p>
 *
 * The {@code Copycat} constructor requires a {@link net.kuujo.copycat.CopycatContext} and
 * {@link net.kuujo.copycat.spi.service.Service}:
 * <p>
 *
 * {@code
 * StateMachine stateMachine = new MyStateMachine();
 * Log log = new MemoryMappedFileLog("data.log");
 * ClusterConfig<Member> config = new LocalClusterConfig();
 * config.setLocalMember("foo");
 * config.setRemoteMembers("bar", "baz");
 * Cluster<Member> cluster = new LocalCluster(config);
 * CopycatContext context = CopycatContext.context(stateMachine, log, cluster);
 * 
 * CopycatService service = new HttpService("localhost", 8080);
 * 
 * Copycat copycat = Copycat.copycat(service, context);
 * copycat.start();
 * }
 * <p>
 *
 * Copycat also exposes a fluent interface for reacting on internal events. This can be useful for
 * detecting cluster membership or leadership changes, for instance:
 * <p>
 *
 * {@code copycat.on().membershipChange(event -> System.out.println("Membership changed: " +
 * event.members()); }); }
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Copycat extends BaseCopycat {
  /**
   * Returns a new copycat builder.
   *
   * @return A new copycat builder.
   */
  @SuppressWarnings("unchecked")
  static BaseCopycat.Builder<Copycat, Protocol<?>> builder() {
    return new BaseCopycat.Builder<>((builder) -> new SyncCopycatImpl(new StateContext(
        builder.stateMachine, builder.log, builder.cluster, builder.protocol, builder.config),
        builder.cluster, builder.config));
  }

  /**
   * Starts the replica.
   */
  void start();

  /**
   * Stops the replica.
   */
  void stop();

  /**
   * Submits an operation to the cluster.
   *
   * @param operation The name of the operation to submit.
   * @param args An ordered list of operation arguments.
   * @return The operation result.
   * @throws NullPointerException if {@code operation} is null
   */
  <R> R submit(String operation, Object... args);

}
