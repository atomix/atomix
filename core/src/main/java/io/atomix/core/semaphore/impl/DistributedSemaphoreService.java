/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.semaphore.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.atomix.core.semaphore.DistributedSemaphoreServiceConfig;
import io.atomix.core.semaphore.QueueStatus;
import io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.Acquire;
import io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.Drain;
import io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.Increase;
import io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.Reduce;
import io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.Release;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.PrimitiveSession;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.ACQUIRE;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.AVAILABLE;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.DRAIN;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.HOLDER_STATUS;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.INCREASE;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.QUEUE_STATUS;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.REDUCE;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.RELEASE;


public class DistributedSemaphoreService extends AbstractPrimitiveService {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(DistributedSemaphoreOperations.NAMESPACE)
      .register(DistributedSemaphoreEvents.NAMESPACE)
      .register(QueueStatus.class)
      .register(Versioned.class)
      .register(Waiter.class)
      .build());

  private int available;
  private Map<Long, Integer> holders = new HashMap<>();
  private LinkedList<Waiter> waiterQueue = new LinkedList<>();
  private final Map<Long, Scheduled> timers = new HashMap<>();

  public DistributedSemaphoreService(DistributedSemaphoreServiceConfig config) {
    super(config);
    this.available = config.initialCapacity();
  }


  @Override
  protected void configure(ServiceExecutor executor) {
    executor.register(ACQUIRE, this::onAcquire);
    executor.register(RELEASE, this::onRelease);
    executor.register(AVAILABLE, this::onAvailable);
    executor.register(DRAIN, this::onDrain);
    executor.register(INCREASE, this::onIncrease);
    executor.register(REDUCE, this::onReduce);
    executor.register(QUEUE_STATUS, this::onQueueStatus);
    executor.register(HOLDER_STATUS, this::onHolderStatus);
  }

  @Override
  public void backup(BackupOutput output) {
    output.writeInt(available);
    output.writeObject(holders, SERIALIZER::encode);
    output.writeObject(waiterQueue, SERIALIZER::encode);
  }

  @Override
  public void restore(BackupInput input) {
    available = input.readInt();

    holders = input.readObject(SERIALIZER::decode);
    waiterQueue = input.readObject(SERIALIZER::decode);

    timers.values().forEach(Scheduled::cancel);
    timers.clear();

    for (Waiter waiter : waiterQueue) {
      if (waiter.expire > 0) {
        timers.put(waiter.index, getScheduler()
            .schedule(Duration.ofMillis(waiter.expire - getWallClock().getTime().unixTimestamp()), () -> {
              timers.remove(waiter.index);
              waiterQueue.remove(waiter);
              fail(waiter.session, waiter.id, waiter.acquirePermits, waiter.index);
            }));
      }
    }
  }

  @Override
  public void onExpire(PrimitiveSession session) {
    releaseSession(session);
  }

  @Override
  public void onClose(PrimitiveSession session) {
    releaseSession(session);
  }

  protected void onAcquire(Commit<Acquire> commit) {
    Acquire acquire = commit.value();
    if (available >= commit.value().permits()) {
      acquire(commit.session().sessionId().id(), acquire.id(), acquire.permits(), commit.index());
    } else {
      if (acquire.timeout() > 0) {
        Waiter waiter = new Waiter(
            commit.session().sessionId().id(),
            commit.index(),
            acquire.id(),
            acquire.permits(),
            getWallClock().getTime().unixTimestamp() + acquire.timeout());
        waiterQueue.add(waiter);
        timers.put(commit.index(), getScheduler().schedule(acquire.timeout(), TimeUnit.MILLISECONDS, () -> {
          timers.remove(commit.index());
          waiterQueue.remove(waiter);
          fail(commit.session().sessionId().id(), acquire.id(), acquire.permits(), commit.index());
        }));
      } else if (acquire.timeout() == 0) {
        fail(commit.session().sessionId().id(), acquire.id(), acquire.permits(), commit.index());
      } else {
        waiterQueue.add(new Waiter(
            commit.session().sessionId().id(),
            commit.index(),
            acquire.id(),
            acquire.permits(),
            0));
      }
    }
  }

  protected void onRelease(Commit<Release> commit) {
    release(commit.session().sessionId().id(), commit.value().permits());
  }

  protected Versioned<Integer> onAvailable(Commit<Void> commit) {
    return new Versioned<>(available, commit.index());
  }

  protected Versioned<Integer> onDrain(Commit<Drain> commit) {
    int acquirePermits = available;
    available = 0;

    if (acquirePermits > 0) {
      holders.compute(commit.session().sessionId().id(), (k, v) -> {
        if (v == null) {
          v = 0;
        }
        return v + acquirePermits;
      });
    }
    return new Versioned<>(acquirePermits, commit.index());
  }

  protected Versioned<Integer> onIncrease(Commit<Increase> commit) {
    increaseAvailable(commit.value().permits());
    checkAndNotifyWaiters();
    return new Versioned<>(available, commit.index());
  }

  protected Versioned<Integer> onReduce(Commit<Reduce> commit) {
    return new Versioned<>(decreaseAvailable(commit.value().permits()), commit.index());
  }

  protected Versioned<QueueStatus> onQueueStatus(Commit<Void> commit) {
    int permits = waiterQueue.stream().map(w -> w.acquirePermits).reduce(0, Integer::sum);
    return new Versioned<>(new QueueStatus(waiterQueue.size(), permits), commit.index());
  }

  protected Versioned<Map<Long, Integer>> onHolderStatus(Commit<Void> commit) {
    return new Versioned<>(holders, commit.index());
  }

  private void acquire(long sessionId, long operationId, int acquirePermits, long version) {
    decreaseAvailable(acquirePermits);
    holders.compute(sessionId, (k, v) -> {
      if (v == null) {
        v = 0;
      }
      return v + acquirePermits;
    });
    success(sessionId, operationId, acquirePermits, version);
  }


  /**
   * Release permits and traverse the queue to remove waiters that meet the requirement.
   *
   * @param sessionId      sessionId
   * @param releasePermits permits to release
   */
  private void release(long sessionId, int releasePermits) {
    increaseAvailable(releasePermits);
    holders.computeIfPresent(sessionId, (id, acquired) -> {
      acquired -= releasePermits;
      if (acquired <= 0) {
        return null;
      }
      return acquired;
    });

    checkAndNotifyWaiters();
  }

  private void success(long sessionId, long operationId, int acquirePermits, long version) {
    PrimitiveSession session = getSession(sessionId);
    if (session != null && session.getState().active()) {
      session.publish(DistributedSemaphoreEvents.SUCCESS, new SemaphoreEvent(operationId, version, acquirePermits));
    }
  }

  private void fail(long sessionId, long operationId, int acquirePermits, long version) {
    PrimitiveSession session = getSession(sessionId);
    if (session != null && session.getState().active()) {
      session.publish(DistributedSemaphoreEvents.FAILED, new SemaphoreEvent(operationId, version, acquirePermits));
    }
  }

  private void releaseSession(PrimitiveSession session) {
    if (holders.containsKey(session.sessionId().id())) {
      release(session.sessionId().id(), holders.get(session.sessionId().id()));
    }
  }

  private void checkAndNotifyWaiters() {
    Iterator<Waiter> iterator = waiterQueue.iterator();
    while (iterator.hasNext() && available > 0) {
      Waiter waiter = iterator.next();
      if (available >= waiter.acquirePermits) {
        iterator.remove();
        Scheduled timer = timers.remove(waiter.index);
        if (timer != null) {
          timer.cancel();
        }
        acquire(waiter.session, waiter.id, waiter.acquirePermits, waiter.index);
      }
    }
  }

  private int increaseAvailable(int permits) {
    int newAvailable = available + permits;

    if (newAvailable < available) {
      newAvailable = Integer.MAX_VALUE;
    }
    available = newAvailable;

    return available;
  }

  private int decreaseAvailable(int permits) {
    int newAvailable = available - permits;

    if (newAvailable > available) {
      newAvailable = Integer.MIN_VALUE;
    }
    available = newAvailable;

    return available;
  }

  @Override
  public Serializer serializer() {
    return SERIALIZER;
  }

  private class Waiter {
    private final long session;
    private final long index;
    private final long id;
    private final int acquirePermits;
    private final long expire;

    public Waiter(long session, long index, long id, int acquirePermits, long expire) {
      this.session = session;
      this.index = index;
      this.id = id;
      this.acquirePermits = acquirePermits;
      this.expire = expire;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Waiter waiter = (Waiter) o;
      return session == waiter.session &&
          index == waiter.index &&
          id == waiter.id &&
          acquirePermits == waiter.acquirePermits;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(session, index, id, acquirePermits);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("session", session)
          .add("index", index)
          .add("id", id)
          .add("acquirePermits", acquirePermits)
          .add("expire", expire)
          .toString();
    }
  }
}
