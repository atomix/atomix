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
package io.atomix.concurrent;

import io.atomix.catalyst.concurrent.BlockingFuture;
import io.atomix.concurrent.internal.LockCommands;
import io.atomix.concurrent.util.DistributedLockFactory;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceTypeInfo;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Facilitates synchronizing access to cluster-wide shared resources.
 * <p>
 * The distributed lock resource provides a mechanism for nodes to synchronize access to cluster-wide shared resources.
 * This interface is an asynchronous version of Java's {@link java.util.concurrent.locks.Lock}.
 * <pre>
 *   {@code
 *   atomix.getLock("my-lock").thenAccept(lock -> {
 *     lock.lock().thenRun(() -> {
 *       ...
 *       lock.unlock();
 *     });
 *   });
 *   }
 * </pre>
 * Locks are implemented as a simple replicated queue that tracks which client currently holds a lock. When a lock
 * is {@link #lock() locked}, if the lock is available then the requesting resource instance will immediately receive
 * the lock, otherwise the lock request will be queued. When a lock is {@link #unlock() unlocked}, the next lock requester
 * will be granted the lock.
 * <p>
 * Distributed locks require no polling from the client. Locks are granted via session events published by the Atomix
 * cluster to the lock instance. In the event that a lock's client becomes disconnected from the cluster, its session
 * will expire after the configured cluster session timeout and the lock will be automatically released.
 * <h2>Detecting failures</h2>
 * Once a lock is acquired by a client, the cluster will monitor the lock holder's availability and release the lock
 * automatically if the client becomes disconnected from the cluster. However, in the event that a lock holder becomes
 * disconnected without crashing, it's possible for two processes to believe themselves to hold the lock simultaneously.
 * If the lock holder becomes disconnected the cluster may grant the lock to another process. For this reason it's essential
 * that clients monitor the {@link io.atomix.resource.Resource.State State} of the lock. If the resource transitions to the
 * {@link Resource.State#SUSPENDED} state, that indicates that the underlying client is unable to communicate with the
 * cluster and another process may have been granted the lock. Lock holders should monitor the resource for state changes
 * and release the lock if the resource becomes suspended.
 * <p>
 * <pre>
 *   {@code
 *   DistributedLock lock = atomix.getLock("my-lock").get();
 *
 *   lock.lock().thenRun(() -> {
 *     lock.onStateChange(state -> {
 *       if (state == DistributedLock.State.SUSPENDED) {
 *         lock.unlock();
 *         System.out.println("lost the lock");
 *       }
 *     });
 *     // Do stuff
 *   });
 *   }
 * </pre>
 * <h2>Fencing</h2>
 * Detecting state changes in the lock client's session may not always be sufficient for determining whether the
 * local resource instance is the unique lock holder in the cluster. For instance, a lengthy garbage collection
 * pause can result in the client's session expiring without any change to the client's session state, resulting
 * in another process being granted the lock. To account for arbitrary lapses in time, the {@code DistributedLock}
 * provides a monotonically increasing, globally unique identifier to each process granted the lock. The token can
 * be used for optimistic concurrency control when accessing external resources.
 * <pre>
 *   {@code
 *   lock.lock().thenAccept(token -> {
 *     // Write to external data store, checking that the last written token is not greater than the current token
 *   });
 *   }
 * </pre>
 * This is known as a fencing token. When using the lock to control concurrent writes to e.g. an external data
 * store, the monotonically increasing identifier provided when the lock is granted allows lock holders to
 * optimistically determine whether the lock has been granted to a more recent lock requester that has written
 * to the data store. If the last write to the external data store is greater than the local lock's token, that
 * indicates that another process has been granted the lock.
 * <h3>Implementation</h3>
 * Lock state management is implemented in a Copycat replicated {@link io.atomix.copycat.server.StateMachine}.
 * When a lock is created, an instance of the lock state machine is created on each replica in the cluster.
 * The state machine instance manages state for the specific lock. When a client makes a {@link #lock()}
 * request, it submits the request with a monotonically increasing identifier unique to the resource instance.
 * If the lock is not currently held by another process, the state machine grants the lock to the
 * requester and {@link io.atomix.copycat.server.session.ServerSession#publish(String, Object) publishes}
 * a {@code lock} event with the requested lock ID and a globally unique, monotonically increasing token to
 * the client. If the lock is held by another process, the lock request is enqueued to await availability of
 * the lock.
 * <p>
 * When a client-side {@code DistributedLock} receives a published {@code lock} event from the cluster, the
 * client associates the lock ID with a lock requested by the client and grants the lock to the requester.
 * The globally unique, monotonically increasing token provided by the cluster is provided to the user as
 * a fencing token. Because of garbage collection and other types of process pauses, we cannot guarantee that
 * two clients cannot perceive themselves to hold the same lock at the same time. The fencing token provides
 * a mechanism for optimistic locking when interacting with external resources.
 * <p>
 * When a lock holder {@link #unlock() unlocks} a lock, a second request is sent to the cluster. The {@code unlock}
 * request is logged and replicated to a majority of the cluster and applied to the replicated state machine.
 * If another lock requester is awaiting the lock, the state machine will grant the lock to the next requester
 * in the queue.
 * <p>
 * Because a lock holder may become partitioned or crash before releasing a lock, the lock state machine is responsible
 * for tracking which session currently holds the lock. In the event that the lock holder is partitioned or crashes,
 * its session will eventually be expired by the state machine, and the lock will be granted to the next requester
 * in the queue.
 * <p>
 * The lock state machine manages compaction in the replicated log by tracking which {@code lock} and {@code unlock}
 * requests contribute to the state of the lock. As long as a client holds a lock, the commit requesting the lock
 * will be retained in the replicated log. And as long as a client's request to acquire a lock is held in the state
 * machine's lock queue, the commit requesting the lock will be retained in the replicated log. This ensures that if
 * a replica crashes and recovers it will recover the correct state of the lock. Once a lock is released by a client,
 * both the {@code lock} and {@code unlock} commit associated with the lock will be released from the state machine
 * and eventually removed from the log during compaction.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-22, factory=DistributedLockFactory.class)
public class DistributedLock extends AbstractResource<DistributedLock> {
  private final Map<Integer, CompletableFuture<Long>> futures = new ConcurrentHashMap<>();
  private final AtomicInteger id = new AtomicInteger();
  private int lock;

  public DistributedLock(CopycatClient client, Properties options) {
    super(client, options);
  }

  @Override
  public CompletableFuture<DistributedLock> open() {
    return super.open().thenApply(result -> {
      client.onEvent("lock", this::handleEvent);
      client.onEvent("fail", this::handleFail);
      return result;
    });
  }

  /**
   * Handles a received lock event.
   */
  private void handleEvent(LockCommands.LockEvent event) {
    CompletableFuture<Long> future = futures.get(event.id());
    if (future != null) {
      this.lock = event.id();
      future.complete(event.version());
    }
  }

  /**
   * Handles a received failure event.
   */
  private void handleFail(LockCommands.LockEvent event) {
    CompletableFuture<Long> future = futures.get(event.id());
    if (future != null) {
      future.complete(null);
    }
  }

  /**
   * Acquires the lock.
   * <p>
   * When the lock is acquired, this lock instance will publish a lock request to the cluster and await
   * an event granting the lock to this instance. The returned {@link CompletableFuture} will not be completed
   * until the lock has been acquired.
   * <p>
   * Once the lock is granted, the returned future will be completed with a positive {@code Long} value. This value
   * is guaranteed to be unique across all clients and monotonically increasing. Thus, the value can be used as a
   * fencing token for further concurrency control.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   lock.lock().join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   lock.lock().thenRun(() -> System.out.println("Lock acquired!"));
   *   }
   * </pre>
   *
   * @return A completable future to be completed once the lock has been acquired.
   */
  public CompletableFuture<Long> lock() {
    CompletableFuture<Long> future = new BlockingFuture<>();
    int id = this.id.incrementAndGet();
    futures.put(id, future);
    client.submit(new LockCommands.Lock(id, -1)).whenComplete((result, error) -> {
      if (error != null) {
        futures.remove(id);
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Attempts to acquire the lock if available.
   * <p>
   * When the lock is acquired, this lock instance will publish an immediate lock request to the cluster. If the
   * lock is available, the lock will be granted and the returned {@link CompletableFuture} will be completed
   * successfully. If the lock is not immediately available, the {@link CompletableFuture} will be completed
   * with a {@code null} value.
   * <p>
   * If the lock is granted, the returned future will be completed with a positive {@code Long} value. This value
   * is guaranteed to be unique across all clients and monotonically increasing. Thus, the value can be used as a
   * fencing token for further concurrency control.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (lock.tryLock().get()) {
   *     System.out.println("Lock acquired!");
   *   } else {
   *     System.out.println("Lock failed!");
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   lock.tryLock().thenAccept(locked -> {
   *     if (locked) {
   *       System.out.println("Lock acquired!");
   *     } else {
   *       System.out.println("Lock failed!");
   *     }
   *   });
   *   }
   * </pre>
   *
   *
   * @return A completable future to be completed with a boolean indicating whether the lock was acquired.
   */
  public CompletableFuture<Long> tryLock() {
    CompletableFuture<Long> future = new BlockingFuture<>();
    int id = this.id.incrementAndGet();
    futures.put(id, future);
    client.submit(new LockCommands.Lock(id, 0)).whenComplete((result, error) -> {
      if (error != null) {
        futures.remove(id);
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Attempts to acquire the lock if available within the given timeout.
   * <p>
   * When the lock is acquired, this lock instance will publish an immediate lock request to the cluster. If the
   * lock is available, the lock will be granted and the returned {@link CompletableFuture} will be completed
   * successfully. If the lock is not immediately available, the lock request will be queued until the lock comes
   * available. If the lock {@code timeout} expires, the lock request will be cancelled and the returned
   * {@link CompletableFuture} will be completed successfully with a {@code null} result.
   * <p>
   * If the lock is granted, the returned future will be completed with a positive {@code Long} value. This value
   * is guaranteed to be unique across all clients and monotonically increasing. Thus, the value can be used as a
   * fencing token for further concurrency control.
   * <p>
   * <b>Timeouts and wall-clock time</b>
   * The provided {@code timeout} may not ultimately be representative of the actual timeout in the cluster. Because
   * of clock skew and determinism requirements, the actual timeout may be arbitrarily greater, but not less, than
   * the provided {@code timeout}. However, time will always progress monotonically. That is, time will never go
   * in reverse. A timeout of {@code 10} seconds will always be greater than a timeout of {@code 9} seconds, but the
   * times simply may not match actual wall-clock time.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (lock.tryLock(Duration.ofSeconds(10)).get()) {
   *     System.out.println("Lock acquired!");
   *   } else {
   *     System.out.println("Lock failed!");
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   lock.tryLock(Duration.ofSeconds(10)).thenAccept(locked -> {
   *     if (locked) {
   *       System.out.println("Lock acquired!");
   *     } else {
   *       System.out.println("Lock failed!");
   *     }
   *   });
   *   }
   * </pre>
   *
   * @param timeout The duration within which to acquire the lock.
   * @return A completable future to be completed with a value indicating whether the lock was acquired.
   */
  public CompletableFuture<Long> tryLock(Duration timeout) {
    CompletableFuture<Long> future = new BlockingFuture<>();
    int id = this.id.incrementAndGet();
    futures.put(id, future);
    client.submit(new LockCommands.Lock(id, timeout.toMillis())).whenComplete((result, error) -> {
      if (error != null) {
        futures.remove(id);
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Releases the lock.
   * <p>
   * When the lock is released, the lock instance will publish an unlock request to the cluster. Once the lock has
   * been released, if any other instances of this resource are waiting for a lock on this or another node, the lock
   * will be acquired by the waiting instance before this unlock operation is completed. Once the lock has been released
   * and granted to any waiters, the returned {@link CompletableFuture} will be completed.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   lock.unlock().join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   lock.unlock().thenRun(() -> System.out.println("Lock released!"));
   *   }
   * </pre>
   *
   * @return A completable future to be completed once the lock has been released.
   */
  public CompletableFuture<Void> unlock() {
    int lock = this.lock;
    this.lock = 0;
    if (lock != 0) {
      return client.submit(new LockCommands.Unlock(lock));
    }
    return CompletableFuture.completedFuture(null);
  }

}
