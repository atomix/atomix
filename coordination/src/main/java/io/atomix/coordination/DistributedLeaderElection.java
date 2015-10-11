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
package io.atomix.coordination;

import io.atomix.DistributedResource;
import io.atomix.catalyst.util.Listener;
import io.atomix.coordination.state.LeaderElectionCommands;
import io.atomix.coordination.state.LeaderElectionState;
import io.atomix.copycat.server.StateMachine;
import io.atomix.resource.ResourceContext;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Provides a mechanism for coordinating cluster-wide resources with a strong leader.
 * <p>
 * Leader election allows a set of distributed processes to coordinate access to and control over resources by
 * electing a single process to do work. This leader election implementation uses a consensus-based state machine
 * to automatically rotate leaders as necessary.
 * <p>
 * To create a leader election, use the {@code DistributedLeaderElection} resource class when constructing a resource
 * instance:
 * <pre>
 *   {@code
 *   DistributedLeaderElection election = atomix.create("election", DistributedLeaderElection::new);
 *   }
 * </pre>
 * Leaders are elected by simply registering an election callback on a leader election resource instance:
 * <pre>
 *   {@code
 *   election.onElection(epoch -> {
 *     // do stuff...
 *   });
 *   }
 * </pre>
 * The first election instance that registers an election callback will automatically be elected the leader.
 * If the elected leader becomes disconnected from the cluster or crashes, a new leader will automatically be
 * elected by selecting the next available listener. Thus, the oldest instance is always guaranteed to be
 * the leader.
 * <p>
 * When a leader is elected, the election callback will be supplied with a monotonically increasing election
 * number known commonly as an <em>epoch</em>. The epoch is guaranteed to be unique across the entire cluster
 * and over the full lifetime of a resource for any given election.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DistributedLeaderElection extends DistributedResource<DistributedLeaderElection> {
  private final Set<Consumer<Long>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return LeaderElectionState.class;
  }

  @Override
  protected void open(ResourceContext context) {
    super.open(context);
    context.session().<Long>onEvent("elect", epoch -> {
      for (Consumer<Long> listener : listeners) {
        listener.accept(epoch);
      }
    });
  }

  /**
   * Registers a listener to be called when this instance is elected.
   * <p>
   * If no leader currently exists for this resource, this instance will be elected leader and the provided
   * {@code listener} will be completed <em>before</em> the returned {@link CompletableFuture}. If a leader
   * already exists for the resource, this resource's listener will be queued until all prior listeners
   * have failed or otherwise been lost.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the listener has been registered
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   election.onElection(epoch -> {
   *     ...
   *   }).join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   election.onElection(epoch -> {
   *     ...
   *   }).thenRun(() -> System.out.println("Waiting for election..."));
   *   }
   * </pre>
   *
   * @param listener The listener to register.
   * @return A completable future to be completed with the listener context.
   */
  public CompletableFuture<Listener<Long>> onElection(Consumer<Long> listener) {
    if (!listeners.isEmpty()) {
      listeners.add(listener);
      return CompletableFuture.completedFuture(new ElectionListener(listener));
    }

    listeners.add(listener);
    return submit(new LeaderElectionCommands.Listen())
      .thenApply(v -> new ElectionListener(listener));
  }

  /**
   * Verifies that this instance is the current leader.
   *
   * @param epoch The epoch for which to check if this instance is the leader.
   * @return A completable future to be completed with a boolean value indicating whether the
   *         instance is the current leader.
   */
  public CompletableFuture<Boolean> isLeader(long epoch) {
    return submit(new LeaderElectionCommands.IsLeader(epoch));
  }

  /**
   * Election listener context.
   */
  private class ElectionListener implements Listener<Long> {
    private final Consumer<Long> listener;

    private ElectionListener(Consumer<Long> listener) {
      this.listener = listener;
    }

    @Override
    public void accept(Long epoch) {
      listener.accept(epoch);
    }

    @Override
    public void close() {
      synchronized (DistributedLeaderElection.this) {
        listeners.remove(listener);
        if (listeners.isEmpty()) {
          submit(new LeaderElectionCommands.Unlisten());
        }
      }
    }
  }

}
