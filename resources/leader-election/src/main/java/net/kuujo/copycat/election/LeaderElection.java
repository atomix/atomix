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
package net.kuujo.copycat.election;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.protocol.LeaderChangeEvent;
import net.kuujo.copycat.resource.DiscreteResource;
import net.kuujo.copycat.resource.DiscreteResourceConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Leader election.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderElection extends DiscreteResource<LeaderElection, LeaderElection> {
  private final Map<EventListener<LeaderElectionEvent>, EventListener<LeaderChangeEvent>> listeners = new ConcurrentHashMap<>();

  public LeaderElection(DiscreteResourceConfig config) {
    super(config);
  }

  /**
   * Registers a leader election listener.
   *
   * @param listener The leader election listener.
   * @return The leader election.
   */
  public LeaderElection addElectionListener(EventListener<LeaderElectionEvent> listener) {
    EventListener<LeaderChangeEvent> wrappedListener = event -> {
      if (event.newLeader() != null && event.newLeader().equals(cluster.member())) {
        listener.accept(new LeaderElectionEvent(cluster));
      }
    };
    listeners.put(listener, wrappedListener);
    protocol.addListener(wrappedListener);
    return this;
  }

  /**
   * Unregisters a leader election listener.
   *
   * @param listener The leader election listener.
   * @return The leader election.
   */
  public LeaderElection removeElectionListener(EventListener<LeaderElectionEvent> listener) {
    EventListener<LeaderChangeEvent> wrappedListener = listeners.remove(listener);
    if (wrappedListener != null) {
      protocol.removeListener(wrappedListener);
    }
    return this;
  }

  @Override
  protected Buffer commit(Buffer key, Buffer entry, Buffer result) {
    throw new UnsupportedOperationException();
  }

  /**
   * Leader election builder.
   */
  public static class Builder extends DiscreteResource.Builder<Builder, LeaderElection> {
    private final DiscreteResourceConfig config;

    private Builder() {
      this(new DiscreteResourceConfig() {});
    }

    private Builder(DiscreteResourceConfig config) {
      super(config);
      this.config = config;
    }

    @Override
    public LeaderElection build() {
      return new LeaderElection(config);
    }
  }

}
