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

import io.atomix.catalyst.util.Listener;
import io.atomix.coordination.state.LeaderElectionCommands;
import io.atomix.coordination.state.LeaderElectionState;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceTypeInfo;

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
 *   DistributedLeaderElection election = atomix.create("election", DistributedLeaderElection.TYPE);
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
 * <h3>Detecting failures</h3>
 * Once an instance is elected leader, the cluster will track the leader's availability and elect a new leader
 * in the event that the leader becomes disconnected from the cluster. However, in order to ensure no two processes
 * believe themselves to be the leader simultaneously, the leader itself may need to take additional measures to
 * detect failures. If the resource's {@link DistributedLeaderElection#state() State} transitions to
 * {@link io.atomix.resource.Resource.State#SUSPENDED}, that indicates that the client cannot communicate with the
 * cluster. In that case, a new leader may be elected after some time. Users should assume that the
 * {@link io.atomix.resource.Resource.State#SUSPENDED SUSPENDED} state is indicative of a loss of leadership. To listen
 * for a leader state change, register a {@link DistributedLeaderElection#onStateChange(Consumer) state change listener}
 * once the leader is elected.
 * <p>
 * <pre>
 *   {@code
 *   DistributedLeaderElection election = atomix.create("election", DistributedLeaderElection.TYPE).get();
 *   election.onElection(epoch -> {
 *     election.onStateChange(state -> {
 *       if (state == DistributedLeaderElection.State.SUSPENDED) {
 *         election.resign(epoch);
 *         System.out.println("lost leadership");
 *       }
 *     });
 *   });
 *   }
 * </pre>
 * In the event that the resource is able to maintain its session, the cluster may not elect a new leader until
 * the existing leader resigns. For this reason, clients should {@link #resign(long)} their leadership explicitly
 * when a {@link DistributedLeaderElection.State#SUSPENDED SUSPENDED} state is detected. Alternatively, leaders
 * can poll the cluster to determine whether they're still the leader for the resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-21, stateMachine=LeaderElectionState.class)
public class DistributedLeaderElection extends Resource<DistributedLeaderElection, Resource.Options> {
  private final Set<Consumer<Long>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private volatile long epoch;

  public DistributedLeaderElection(CopycatClient client, Resource.Options options) {
    super(client, options);
  }

  @Override
  public CompletableFuture<DistributedLeaderElection> open() {
    return super.open().thenRun(() -> {
      client.<Long>onEvent("elect", epoch -> {
        this.epoch = epoch;
        for (Consumer<Long> listener : listeners) {
          listener.accept(epoch);
        }
      });
    }).thenCompose(v -> submit(new LeaderElectionCommands.Listen()))
      .thenApply(v -> this);
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
   * @param callback The callback to register.
   * @return A completable future to be completed with the listener context.
   */
  public Listener<Long> onElection(Consumer<Long> callback) {
    Listener<Long> listener = new Listener<Long>() {
      @Override
      public void accept(Long epoch) {
        callback.accept(epoch);
      }
      @Override
      public void close() {
        listeners.remove(this);
      }
    };
    listeners.add(listener);

    if (epoch != 0) {
      listener.accept(epoch);
    }
    return listener;
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
   * Resigns leadership for the given epoch.
   * <p>
   * The epoch indicates the election for which to resign as leader and should be provided from
   * the argument passed to the election callback.
   * <p>
   * <pre>
   *   {@code
   *   election.onElection(epoch -> {
   *     election.resign(epoch);
   *   });
   *   }
   * </pre>
   *
   * @param epoch The epoch for which to resign.
   * @return A completable future to be completed once the instance has resigned from leadership for the given epoch.
   */
  public CompletableFuture<Void> resign(long epoch) {
    return submit(new LeaderElectionCommands.Resign(epoch))
      .whenComplete((result, error) -> this.epoch = 0);
  }

}
