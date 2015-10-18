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

import io.atomix.coordination.state.LockCommands;
import io.atomix.coordination.state.LockState;
import io.atomix.copycat.client.RaftClient;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.Consistency;
import io.atomix.resource.ResourceInfo;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * Provides a mechanism for synchronizing access to cluster-wide shared resources.
 * <p>
 * The distributed lock resource provides a mechanism for nodes to synchronize access to cluster-wide shared resources.
 * This interface is an asynchronous version of Java's {@link java.util.concurrent.locks.Lock}.
 * <pre>
 *   {@code
 *   atomix.create("lock", DistributedLock::new).thenAccept(lock -> {
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
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@ResourceInfo(stateMachine=LockState.class)
public class DistributedLock extends AbstractResource {
  private final Queue<Consumer<Boolean>> queue = new ConcurrentLinkedQueue<>();

  public DistributedLock(RaftClient client) {
    super(client);
    client.session().onEvent("lock", this::handleEvent);
  }

  @Override
  public DistributedLock with(Consistency consistency) {
    super.with(consistency);
    return this;
  }

  /**
   * Handles a received session event.
   */
  private void handleEvent(boolean locked) {
    Consumer<Boolean> consumer = queue.poll();
    if (consumer != null) {
      consumer.accept(locked);
    }
  }

  /**
   * Acquires the lock.
   * <p>
   * When the lock is acquired, this lock instance will publish a lock request to the cluster and await
   * an event granting the lock to this instance. The returned {@link CompletableFuture} will not be completed
   * until the lock has been acquired.
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
  public CompletableFuture<Void> lock() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Consumer<Boolean> consumer = locked -> future.complete(null);
    queue.add(consumer);
    submit(new LockCommands.Lock(-1)).whenComplete((result, error) -> {
      if (error != null) {
        queue.remove(consumer);
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
   * {@code false}.
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
  public CompletableFuture<Boolean> tryLock() {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    Consumer<Boolean> consumer = future::complete;
    queue.add(consumer);
    submit(new LockCommands.Lock()).whenComplete((result, error) -> {
      if (error != null) {
        queue.remove(consumer);
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
   * {@link CompletableFuture} will be completed {@code false}.
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
   * @return A completable future to be completed with a boolean indicating whether the lock was acquired.
   */
  public CompletableFuture<Boolean> tryLock(Duration timeout) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    Consumer<Boolean> consumer = future::complete;
    queue.add(consumer);
    submit(new LockCommands.Lock(timeout.toMillis())).whenComplete((result, error) -> {
      if (error != null) {
        queue.remove(consumer);
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
    return submit(new LockCommands.Unlock());
  }

}
