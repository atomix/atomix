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
package net.kuujo.copycat;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.endpoint.EndpointFactory;
import net.kuujo.copycat.endpoint.impl.DefaultEndpointFactory;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogFactory;
import net.kuujo.copycat.log.impl.FileLogFactory;
import net.kuujo.copycat.protocol.CorrelationStrategy;
import net.kuujo.copycat.protocol.TimerStrategy;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.registry.impl.BasicRegistry;

/**
 * Primary copycat API.<p>
 *
 * The <code>CopyCat</code> class provides a fluent API for
 * combining the {@link CopyCatContext} with an {@link Endpoint}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCat {
  private final Endpoint endpoint;
  private final CopyCatContext context;

  public CopyCat(Endpoint endpoint, StateMachine stateMachine, ClusterConfig cluster) {
    this(endpoint, new CopyCatContext(stateMachine, cluster));
  }

  public CopyCat(String uri, StateMachine stateMachine, ClusterConfig cluster) {
    this.context = new CopyCatContext(stateMachine, cluster);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
    endpoint.init(context);
  }

  public CopyCat(String uri, StateMachine stateMachine, LogFactory logFactory, ClusterConfig cluster) {
    this.context = new CopyCatContext(stateMachine, logFactory, cluster);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
    endpoint.init(context);
  }

  public CopyCat(String uri, StateMachine stateMachine, LogFactory logFactory, ClusterConfig cluster, CopyCatConfig config) {
    this.context = new CopyCatContext(stateMachine, logFactory, cluster, config);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
    endpoint.init(context);
  }

  public CopyCat(String uri, StateMachine stateMachine, LogFactory logFactory, ClusterConfig cluster, CopyCatConfig config, Registry registry) {
    this.context = new CopyCatContext(stateMachine, logFactory, cluster, config, registry);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
    endpoint.init(context);
  }

  private CopyCat(Endpoint endpoint, CopyCatContext context) {
    this.endpoint = endpoint;
    this.context = context;
    endpoint.init(context);
  }

  /**
   * Starts the replica.
   *
   * @return A completable future to be completed once the replica has started.
   */
  public CompletableFuture<Void> start() {
    return context.start().thenRun(()->{});
  }

  /**
   * Stops the replica.
   *
   * @return A completable future to be completed once the replica has stopped.
   */
  public CompletableFuture<Void> stop() {
    return endpoint.stop();
  }

  /**
   * CopyCat builder.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public static class Builder {
    private Endpoint endpoint;
    private String uri;
    private CopyCatConfig config = new CopyCatConfig();
    private ClusterConfig cluster = new ClusterConfig();
    private StateMachine stateMachine;
    private LogFactory logFactory = new FileLogFactory();
    private Registry registry = new BasicRegistry();

    /**
     * Sets the copycat endpoint.
     *
     * @param uri The copycat endpoint.
     * @return The copycat builder.
     */
    public Builder withEndpoint(String uri) {
      this.uri = uri;
      return this;
    }

    /**
     * Sets the copycat endpoint.
     *
     * @param uri The copycat endpoint.
     * @return The copycat builder.
     */
    public Builder withEndpoint(Endpoint endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    /**
     * Sets the copycat log factory.
     *
     * @param uri The copycat log factory.
     * @return The copycat builder.
     */
    public Builder withLogFactory(LogFactory factory) {
      this.logFactory = factory;
      return this;
    }

    /**
     * Sets the copycat log.
     *
     * @param uri The copycat log.
     * @return The copycat builder.
     */
    public Builder withLog(Log log) {
      this.logFactory = new LogFactory() {
        @Override
        public Log createLog(String name) {
          return log;
        }
      };
      return this;
    }

    /**
     * Sets the copycat configuration.
     *
     * @param uri The copycat configuration.
     * @return The copycat builder.
     */
    public Builder withConfig(CopyCatConfig config) {
      this.config = config;
      return this;
    }

    /**
     * Sets the copycat election timeout.
     *
     * @param uri The copycat election timeout.
     * @return The copycat builder.
     */
    public Builder withElectionTimeout(long timeout) {
      config.setElectionTimeout(timeout);
      return this;
    }

    /**
     * Sets the copycat heartbeat interval.
     *
     * @param uri The copycat heartbeat interval.
     * @return The copycat builder.
     */
    public Builder withHeartbeatInterval(long interval) {
      config.setHeartbeatInterval(interval);
      return this;
    }

    /**
     * Sets whether to require quorums during reads.
     *
     * @param requireQuorum Whether to require quorums during reads.
     * @return The copycat builder.
     */
    public Builder withRequireReadQuorum(boolean requireQuorum) {
      config.setRequireReadQuorum(requireQuorum);
      return this;
    }

    /**
     * Sets whether to require quorums during writes.
     *
     * @param requireQuorum Whether to require quorums during writes.
     * @return The copycat builder.
     */
    public Builder withRequireWriteQuorum(boolean requireQuorum) {
      config.setRequireWriteQuorum(requireQuorum);
      return this;
    }

    /**
     * Sets the read quorum size.
     *
     * @param quorumSize The read quorum size.
     * @return The copycat builder.
     */
    public Builder withReadQuorumSize(int quorumSize) {
      config.setReadQuorumSize(quorumSize);
      return this;
    }

    /**
     * Sets the max log size.
     *
     * @param maxSize The max log size.
     * @return The copycat builder.
     */
    public Builder withMaxLogSize(int maxSize) {
      config.setMaxLogSize(maxSize);
      return this;
    }

    /**
     * Sets the correlation strategy.
     *
     * @param strategy The correlation strategy.
     * @return The copycat builder.
     */
    public Builder setCorrelationStrategy(CorrelationStrategy<?> strategy) {
      config.setCorrelationStrategy(strategy);
      return this;
    }

    /**
     * Sets the timer strategy.
     *
     * @param strategy The timer strategy.
     * @return The copycat builder.
     */
    public Builder setTimerStrategy(TimerStrategy strategy) {
      config.setTimerStrategy(strategy);
      return this;
    }

    /**
     * Sets the cluster configuration.
     *
     * @param cluster The cluster configuration.
     * @return The copycat builder.
     */
    public Builder withClusterConfig(ClusterConfig cluster) {
      this.cluster = cluster;
      return this;
    }

    /**
     * Sets the local cluster member.
     *
     * @param uri The local cluster member URI.
     * @return The copycat builder.
     */
    public Builder withLocalMember(String uri) {
      this.cluster.setLocalMember(uri);
      return this;
    }

    /**
     * Sets the remote cluster members.
     *
     * @param uris The remote cluster member URIs.
     * @return The copycat builder.
     */
    public Builder withRemoteMembers(String... uris) {
      this.cluster.setRemoteMembers(uris);
      return this;
    }

    /**
     * Sets the remote cluster members.
     *
     * @param uris The remote cluster member URIs.
     * @return The copycat builder.
     */
    public Builder withRemoteMembers(Set<String> uris) {
      this.cluster.setRemoteMembers(uris);
      return this;
    }

    /**
     * Sets the copycat state machine.
     *
     * @param stateMachine The state machine.
     * @return The copycat builder.
     */
    public Builder withStateMachine(StateMachine stateMachine) {
      this.stateMachine = stateMachine;
      return this;
    }

    /**
     * Sets the copycat registry.
     *
     * @param registry The copycat registry.
     * @return The copycat builder.
     */
    public Builder withRegistry(Registry registry) {
      this.registry = registry;
      return this;
    }

    /**
     * Builds the copycat instance.
     *
     * @return The copycat instance.
     */
    public CopyCat build() {
      CopyCatContext context = new CopyCatContext(stateMachine, logFactory, cluster, config, registry);
      if (endpoint == null) {
        endpoint = new DefaultEndpointFactory(context).createEndpoint(uri);
      }
      return new CopyCat(endpoint, context);
    }

  }

}
