/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.service;

import io.atomix.protocols.raft.event.RaftEvent;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessionListener;
import io.atomix.protocols.raft.session.RaftSessions;
import io.atomix.protocols.raft.storage.snapshot.Snapshottable;
import io.atomix.time.WallClockTimestamp;

/**
 * Base class for user-provided Raft state machines.
 * <p>
 * Users should extend this class to create a state machine for use within a {@link RaftServer}.
 * <p>
 * State machines are responsible for handling {@link RaftOperation operations} submitted to the Raft cluster and
 * filtering {@link Commit committed} operations out of the Raft log. The most important rule of state machines is
 * that <em>state machines must be deterministic</em> in order to maintain Raft's consistency guarantees. That is,
 * state machines must not change their behavior based on external influences and have no side effects. Users should
 * <em>never</em> use {@code System} time to control behavior within a state machine.
 * <p>
 * When {@link io.atomix.protocols.raft.RaftCommand commands} and {@link io.atomix.protocols.raft.RaftQuery queries}
 * (i.e. <em>operations</em>) are submitted to the Raft cluster, the {@link RaftServer} will log and replicate them as
 * necessary and, once complete, apply them to the configured state machine.
 * <p>
 * <h3>State machine operations</h3>
 * State machine operations are implemented as methods on the state machine. Operations can be automatically detected
 * by the state machine during setup or can be explicitly registered by overriding the {@link #configure(RaftServiceExecutor)}
 * method. Each operation method must take a single {@link Commit} argument for a specific operation type.
 * <pre>
 *   {@code
 *   public class MapStateMachine extends StateMachine {
 *
 *     public Object put(Commit<Put> commit) {
 *       Commit<Put> previous = map.put(commit.operation().key(), commit);
 *       if (previous != null) {
 *         try {
 *           return previous.operation().value();
 *         } finally {
 *           previous.close();
 *         }
 *       }
 *       return null;
 *     }
 *
 *     public Object get(Commit<Get> commit) {
 *       try {
 *         Commit<Put> current = map.get(commit.operation().key());
 *         return current != null ? current.operation().value() : null;
 *       } finally {
 *         commit.close();
 *       }
 *     }
 *   }
 *   }
 * </pre>
 * When operations are applied to the state machine they're wrapped in a {@link Commit} object. The commit provides the
 * context of how the command or query was committed to the cluster, including the log {@link Commit#index()}, the
 * {@link RaftSession} from which the operation was submitted, and the approximate
 * wall-clock {@link Commit#wallClockTime()} at which the commit was written to the Raft log. Note that the commit time is
 * guaranteed to progress monotonically, but it may not be representative of the progress of actual time. See the
 * {@link Commit} documentation for more information.
 * <p>
 * State machine operations are guaranteed to be executed in the order in which they were submitted by the client,
 * always in the same thread, and thus always sequentially. State machines do not need to be thread safe, but they must
 * be deterministic. That is, state machines are guaranteed to see {@link io.atomix.protocols.raft.RaftCommand}s in the
 * same order on all servers, and given the same commands in the same order, all servers' state machines should arrive at
 * the same state with the same output (return value). The return value of each operation callback is the response value
 * that will be sent back to the client.
 * <p>
 * <h3>Deterministic scheduling</h3>
 * The {@link RaftServiceExecutor} is responsible for executing state machine operations sequentially and provides an
 * interface similar to that of {@link java.util.concurrent.ScheduledExecutorService} to allow state machines to schedule
 * time-based callbacks. Because of the determinism requirement, scheduled callbacks are guaranteed to be executed
 * deterministically as well. The executor can be accessed via the {@link #executor} field.
 * See the {@link RaftServiceExecutor} documentation for more information.
 * <pre>
 *   {@code
 *   public void putWithTtl(Commit<PutWithTtl> commit) {
 *     map.put(commit.operation().key(), commit);
 *     executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
 *       map.remove(commit.operation().key()).close();
 *     });
 *   }
 *   }
 * </pre>
 * <p>
 * During command or scheduled callbacks, {@link RaftSessions} can be used to send state machine events back to the client.
 * For instance, a lock state machine might use a client's {@link RaftSession}
 * to send a lock event to the client.
 * <pre>
 *   {@code
 *   public void unlock(Commit<Unlock> commit) {
 *     try {
 *       Commit<Lock> next = queue.poll();
 *       if (next != null) {
 *         next.session().publish("lock");
 *       }
 *     } finally {
 *       commit.close();
 *     }
 *   }
 *   }
 * </pre>
 * Attempts to {@link RaftSession#publish(RaftEvent) publish}
 * events during the execution will result in an {@link IllegalStateException}.
 * <p>
 * Even though state machines on multiple servers may appear to publish the same event, Raft's protocol ensures that only
 * one server ever actually sends the event. Still, it's critical that all state machines publish all events to ensure
 * consistency and fault tolerance. In the event that a server fails after publishing a session event, the client will transparently
 * reconnect to another server and retrieve lost event messages.
 * <p>
 * <h3>Snapshotting</h3>
 * Atomix's implementation of the Raft protocol provides a mechanism for storing and loading
 * snapshots of a state machine's state. Snapshots are images of the state machine's state stored at a specific
 * point in logical time (an {@code index}). To support snapshotting, state machine implementations should implement
 * the {@link Snapshottable} interface.
 * <pre>
 *   {@code
 *   public class ValueStateMachine extends StateMachine implements Snapshottable {
 *     private Object value;
 *
 *     public void set(Commit<SetValue> commit) {
 *       this.value = commit.operation().value();
 *       commit.close();
 *     }
 *
 *     public void snapshot(SnapshotWriter writer) {
 *       writer.writeObject(value);
 *     }
 *   }
 *   }
 * </pre>
 * For snapshottable state machines, Raft will periodically request a {@link io.atomix.protocols.raft.storage.snapshot.Snapshot Snapshot}
 * of the state machine's state by calling the {@link Snapshottable#snapshot(io.atomix.protocols.raft.storage.snapshot.SnapshotWriter)}
 * method. Once the state machine has written a snapshot of its state, Raft will automatically remove all commands
 * associated with the state machine from the underlying log.
 *
 * @see Commit
 * @see ServiceContext
 * @see RaftServiceExecutor
 */
public interface RaftService extends Snapshottable, RaftSessionListener {

  /**
   * Initializes the state machine.
   *
   * @param context The state machine context.
   * @throws NullPointerException if {@code context} is null
   */
  void init(ServiceContext context);

  /**
   * Increments the Raft service time to the given timestamp.
   *
   * @param timestamp the service timestamp
   */
  void tick(WallClockTimestamp timestamp);

  /**
   * Applies a commit to the state machine.
   *
   * @param commit the commit to apply
   * @return the commit result
   */
  byte[] apply(Commit<byte[]> commit);

  /**
   * Closes the state machine.
   */
  default void close() {
  }
}
