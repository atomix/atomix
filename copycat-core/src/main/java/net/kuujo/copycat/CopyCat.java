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
import net.kuujo.copycat.cluster.MemberConfig;
import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.event.*;
import net.kuujo.copycat.impl.DefaultCopycat;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.spi.CorrelationStrategy;
import net.kuujo.copycat.spi.TimerStrategy;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Copycat {

  /**
   * Returns a new copycat builder.
   *
   * @return A new copycat builder.
   */
  static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the context events.
   *
   * @return Context events.
   */
  public EventsContext on();

  /**
   * Returns the context for a specific event.
   *
   * @param event The event for which to return the context.
   * @return The event context.
   */
  public <T extends Event> EventContext<T> on(Class<T> event);

  /**
   * Returns the event handlers registry.
   *
   * @return The event handlers registry.
   */
  public EventHandlersRegistry events();

  /**
   * Returns an event handler registry for a specific event.
   *
   * @param event The event for which to return the registry.
   * @return An event handler registry.
   */
  public <T extends Event> EventHandlerRegistry<T> event(Class<T> event);

  /**
   * Starts the replica.
   *
   * @return A completable future to be completed once the replica has started.
   */
  public CompletableFuture<Void> start();

  /**
   * Stops the replica.
   *
   * @return A completable future to be completed once the replica has stopped.
   */
  public CompletableFuture<Void> stop();

  /**
   * Copycat builder.
   */
  public static class Builder {
    private Endpoint endpoint;
    private final CopycatContext.Builder builder = CopycatContext.builder();

    private Builder() {
    }

    /**
     * Sets the copycat endpoint.
     *
     * @param endpoint The copycat endpoint.
     * @return The copycat builder.
     */
    public Builder withEndpoint(Endpoint endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    /**
     * Sets the copycat log.
     *
     * @param log The copycat log.
     * @return The copycat builder.
     */
    public Builder withLog(Log log) {
      builder.withLog(log);
      return this;
    }

    /**
     * Sets the copycat configuration.
     *
     * @param config The copycat configuration.
     * @return The copycat builder.
     */
    public Builder withConfig(CopycatConfig config) {
      builder.withConfig(config);
      return this;
    }

    /**
     * Sets the copycat election timeout.
     *
     * @param timeout The copycat election timeout.
     * @return The copycat builder.
     */
    public Builder withElectionTimeout(long timeout) {
      builder.withElectionTimeout(timeout);
      return this;
    }

    /**
     * Sets the copycat heartbeat interval.
     *
     * @param interval The copycat heartbeat interval.
     * @return The copycat builder.
     */
    public Builder withHeartbeatInterval(long interval) {
      builder.withHeartbeatInterval(interval);
      return this;
    }

    /**
     * Sets whether to require quorums during reads.
     *
     * @param requireQuorum Whether to require quorums during reads.
     * @return The copycat builder.
     */
    public Builder withRequireReadQuorum(boolean requireQuorum) {
      builder.withRequireReadQuorum(requireQuorum);
      return this;
    }

    /**
     * Sets whether to require quorums during writes.
     *
     * @param requireQuorum Whether to require quorums during writes.
     * @return The copycat builder.
     */
    public Builder withRequireWriteQuorum(boolean requireQuorum) {
      builder.withRequireWriteQuorum(requireQuorum);
      return this;
    }

    /**
     * Sets the read quorum size.
     *
     * @param quorumSize The read quorum size.
     * @return The copycat builder.
     */
    public Builder withReadQuorumSize(int quorumSize) {
      builder.withReadQuorumSize(quorumSize);
      return this;
    }

    /**
     * Sets the max log size.
     *
     * @param maxSize The max log size.
     * @return The copycat builder.
     */
    public Builder withMaxLogSize(int maxSize) {
      builder.withMaxLogSize(maxSize);
      return this;
    }

    /**
     * Sets the correlation strategy.
     *
     * @param strategy The correlation strategy.
     * @return The copycat builder.
     */
    public Builder withCorrelationStrategy(CorrelationStrategy<?> strategy) {
      builder.withCorrelationStrategy(strategy);
      return this;
    }

    /**
     * Sets the timer strategy.
     *
     * @param strategy The timer strategy.
     * @return The copycat builder.
     */
    public Builder withTimerStrategy(TimerStrategy strategy) {
      builder.withTimerStrategy(strategy);
      return this;
    }

    /**
     * Sets the cluster configuration.
     *
     * @param cluster The cluster configuration.
     * @return The copycat builder.
     */
    public Builder withClusterConfig(ClusterConfig<?> cluster) {
      builder.withClusterConfig(cluster);
      return this;
    }

    /**
     * Sets the local cluster member.
     *
     * @param member The local cluster member configuration.
     * @return The copycat builder.
     */
    @SuppressWarnings("unchecked")
    public Builder withLocalMember(MemberConfig member) {
      builder.withLocalMember(member);
      return this;
    }

    /**
     * Sets the remote cluster members.
     *
     * @param members The remote cluster member configurations.
     * @return The copycat builder.
     */
    @SuppressWarnings("unchecked")
    public Builder withRemoteMembers(MemberConfig... members) {
      builder.withRemoteMembers(members);
      return this;
    }

    /**
     * Sets the remote cluster members.
     *
     * @param members The remote cluster member configurations.
     * @return The copycat builder.
     */
    @SuppressWarnings("unchecked")
    public Builder withRemoteMembers(Collection<MemberConfig> members) {
      builder.withRemoteMembers(members);
      return this;
    }

    /**
     * Sets the copycat state machine.
     *
     * @param stateMachine The state machine.
     * @return The copycat builder.
     */
    public Builder withStateMachine(StateMachine stateMachine) {
      builder.withStateMachine(stateMachine);
      return this;
    }

    /**
     * Builds the copycat instance.
     *
     * @return The copycat instance.
     */
    public Copycat build() {
      CopycatContext context = builder.build();
      return new DefaultCopycat(endpoint, context);
    }

  }

}
