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
package net.kuujo.copycat.coordination;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.Stateful;
import net.kuujo.copycat.log.Compaction;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Raft;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.Filter;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Asynchronous lock.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(DistributedLock.StateMachine.class)
public class DistributedLock extends Resource {
  private final Queue<Consumer<Boolean>> queue = new ConcurrentLinkedQueue<>();

  public DistributedLock(Raft protocol) {
    super(protocol);
    protocol.session().onReceive(this::receive);
  }

  /**
   * Handles a received session message.
   */
  private void receive(boolean locked) {
    Consumer<Boolean> consumer = queue.poll();
    if (consumer != null) {
      consumer.accept(locked);
    }
  }

  /**
   * Acquires the lock.
   *
   * @return A completable future to be completed once the lock has been acquired.
   */
  public CompletableFuture<Void> lock() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Consumer<Boolean> consumer = locked -> future.complete(null);
    queue.add(consumer);
    submit(Lock.builder().withTimeout(-1).build()).whenComplete((result, error) -> {
      if (error != null) {
        queue.remove(consumer);
      }
    });
    return future;
  }

  /**
   * Acquires the lock if it's free.
   *
   * @return A completable future to be completed with a boolean indicating whether the lock was acquired.
   */
  public CompletableFuture<Boolean> tryLock() {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    Consumer<Boolean> consumer = future::complete;
    queue.add(consumer);
    submit(Lock.builder().build()).whenComplete((result, error) -> {
      if (error != null) {
        queue.remove(consumer);
      }
    });
    return future;
  }

  /**
   * Acquires the lock if it's free within the given timeout.
   *
   * @param time The time within which to acquire the lock in milliseconds.
   * @return A completable future to be completed with a boolean indicating whether the lock was acquired.
   */
  public CompletableFuture<Boolean> tryLock(long time) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    Consumer<Boolean> consumer = future::complete;
    queue.add(consumer);
    submit(Lock.builder().withTimeout(time).build()).whenComplete((result, error) -> {
      if (error != null) {
        queue.remove(consumer);
      }
    });
    return future;
  }

  /**
   * Acquires the lock if it's free within the given timeout.
   *
   * @param time The time within which to acquire the lock.
   * @param unit The time unit.
   * @return A completable future to be completed with a boolean indicating whether the lock was acquired.
   */
  public CompletableFuture<Boolean> tryLock(long time, TimeUnit unit) {
    return submit(Lock.builder().withTimeout(time, unit).build());
  }

  /**
   * Releases the lock.
   *
   * @return A completable future to be completed once the lock has been released.
   */
  public CompletableFuture<Void> unlock() {
    return submit(Unlock.builder().build());
  }

  /**
   * Abstract lock command.
   */
  public static abstract class LockCommand<V> implements Command<V>, AlleycatSerializable {
    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
    }

    /**
     * Base reference command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends LockCommand<V>, V> extends Command.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }
    }
  }

  /**
   * Lock command.
   */
  @SerializeWith(id=512)
  public static class Lock extends LockCommand<Boolean> {

    /**
     * Returns a new lock command builder.
     *
     * @return A new lock command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private long timeout;

    /**
     * Returns the try lock timeout.
     *
     * @return The try lock timeout in milliseconds.
     */
    public long timeout() {
      return timeout;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      buffer.writeLong(timeout);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      timeout = buffer.readLong();
    }

    /**
     * Try lock builder.
     */
    public static class Builder extends LockCommand.Builder<Builder, Lock, Boolean> {
      public Builder(BuilderPool<Builder, Lock> pool) {
        super(pool);
      }

      @Override
      protected void reset(Lock command) {
        super.reset(command);
        command.timeout = 0;
      }

      /**
       * Sets the lock timeout.
       *
       * @param timeout The lock timeout in milliseconds.
       * @return The command builder.
       */
      public Builder withTimeout(long timeout) {
        if (timeout < -1)
          throw new IllegalArgumentException("timeout cannot be less than -1");
        command.timeout = timeout;
        return this;
      }

      /**
       * Sets the lock timeout.
       *
       * @param timeout The lock timeout.
       * @param unit The lock timeout time unit.
       * @return The command builder.
       */
      public Builder withTimeout(long timeout, TimeUnit unit) {
        return withTimeout(unit.toMillis(timeout));
      }

      @Override
      protected Lock create() {
        return new Lock();
      }
    }
  }

  /**
   * Unlock command.
   */
  @SerializeWith(id=513)
  public static class Unlock extends LockCommand<Void> {

    /**
     * Returns a new unlock command builder.
     *
     * @return A new unlock command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Unlock command builder.
     */
    public static class Builder extends LockCommand.Builder<Builder, Unlock, Void> {
      public Builder(BuilderPool<Builder, Unlock> pool) {
        super(pool);
      }

      @Override
      protected Unlock create() {
        return new Unlock();
      }
    }
  }

  /**
   * Asynchronous lock state machine.
   */
  public static class StateMachine extends net.kuujo.copycat.raft.server.StateMachine {
    private long version;
    private long time;
    private Commit<Lock> lock;
    private final Queue<Commit<Lock>> queue = new ArrayDeque<>();

    /**
     * Updates the current time.
     */
    private void updateTime(Commit<?> commit) {
      this.time = commit.timestamp();
    }

    /**
     * Applies a lock commit.
     */
    @Apply(Lock.class)
    protected void applyLock(Commit<Lock> commit) {
      updateTime(commit);
      if (lock == null) {
        lock = commit;
        commit.session().publish(true);
        version = commit.index();
      } else if (commit.operation().timeout() == 0) {
        commit.session().publish(false);
        version = commit.index();
      } else {
        queue.add(commit);
      }
    }

    /**
     * Applies an unlock commit.
     */
    @Apply(Unlock.class)
    protected void applyUnlock(Commit<Unlock> commit) {
      updateTime(commit);

      if (lock == null)
        throw new IllegalStateException("not locked");
      if (!lock.session().equals(commit.session()))
        throw new IllegalStateException("not the lock holder");

      lock = queue.poll();
      while (lock != null && (lock.operation().timeout() != -1 && lock.timestamp() + lock.operation().timeout() < time)) {
        version = lock.index();
        lock = queue.poll();
      }

      if (lock != null) {
        lock.session().publish(true);
        version = lock.index();
      }
    }

    /**
     * Filters a lock commit.
     */
    @Filter(Lock.class)
    protected boolean filterLock(Commit<Lock> commit, Compaction compaction) {
      return commit.index() >= version;
    }

    /**
     * Filters an unlock commit.
     */
    @Filter(Lock.class)
    protected boolean filterUnlock(Commit<Unlock> commit, Compaction compaction) {
      return commit.index() >= version;
    }

  }

}
