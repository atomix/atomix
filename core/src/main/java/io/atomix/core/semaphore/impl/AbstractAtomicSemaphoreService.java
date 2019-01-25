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
import io.atomix.core.semaphore.AtomicSemaphoreType;
import io.atomix.core.semaphore.QueueStatus;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class AbstractAtomicSemaphoreService extends AbstractPrimitiveService<AtomicSemaphoreClient> implements AtomicSemaphoreService {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(AtomicSemaphoreType.instance().namespace())
      .register(Waiter.class)
      .build());

  private int available;
  private Map<Long, Integer> holders = new HashMap<>();
  private LinkedList<Waiter> waiterQueue = new LinkedList<>();
  private final Map<Long, Scheduled> timers = new HashMap<>();

  public AbstractAtomicSemaphoreService(PrimitiveType primitiveType, int initialCapacity) {
    super(primitiveType, AtomicSemaphoreClient.class);
    this.available = initialCapacity;
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
              fail(waiter.session, waiter.id);
            }));
      }
    }
  }

  @Override
  public void onExpire(Session session) {
    releaseSession(session);
  }

  @Override
  public void onClose(Session session) {
    releaseSession(session);
  }

  @Override
  public void acquire(long id, int permits, long timeout) {
    Session session = getCurrentSession();
    if (available >= permits) {
      acquire(session.sessionId(), id, permits, getCurrentIndex());
    } else {
      if (timeout > 0) {
        Waiter waiter = new Waiter(
            session.sessionId(),
            getCurrentIndex(),
            id,
            permits,
            getWallClock().getTime().unixTimestamp() + timeout);
        waiterQueue.add(waiter);

        timers.put(getCurrentIndex(), getScheduler().schedule(timeout, TimeUnit.MILLISECONDS, () -> {
          timers.remove(getCurrentIndex());
          waiterQueue.remove(waiter);
          fail(session.sessionId(), id);
        }));
      } else if (timeout == 0) {
        fail(session.sessionId(), id);
      } else {
        waiterQueue.add(new Waiter(
            session.sessionId(),
            getCurrentIndex(),
            id,
            permits,
            0));
      }
    }
  }

  @Override
  public void release(int permits) {
    release(getCurrentSession().sessionId().id(), permits);
  }

  @Override
  public int available() {
    return available;
  }

  @Override
  public int drain() {
    int acquirePermits = available;
    available = 0;

    if (acquirePermits > 0) {
      holders.compute(getCurrentSession().sessionId().id(), (k, v) -> {
        if (v == null) {
          v = 0;
        }
        return v + acquirePermits;
      });
    }
    return acquirePermits;
  }

  @Override
  public int increase(int permits) {
    increaseAvailable(permits);
    checkAndNotifyWaiters();
    return available;
  }

  @Override
  public int reduce(int permits) {
    return decreaseAvailable(permits);
  }

  @Override
  public QueueStatus queueStatus() {
    int permits = waiterQueue.stream().map(w -> w.acquirePermits).reduce(0, Integer::sum);
    return new QueueStatus(waiterQueue.size(), permits);
  }

  @Override
  public Map<Long, Integer> holderStatus() {
    return holders;
  }

  private void acquire(SessionId sessionId, long operationId, int acquirePermits, long version) {
    decreaseAvailable(acquirePermits);
    holders.compute(sessionId.id(), (k, v) -> {
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

  private void success(SessionId sessionId, long operationId, int acquirePermits, long version) {
    getSession(sessionId).accept(client -> client.succeeded(operationId, version, acquirePermits));
  }

  private void fail(SessionId sessionId, long operationId) {
    getSession(sessionId).accept(client -> client.failed(operationId));
  }

  private void releaseSession(Session session) {
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
    private final SessionId session;
    private final long index;
    private final long id;
    private final int acquirePermits;
    private final long expire;

    Waiter(SessionId session, long index, long id, int acquirePermits, long expire) {
      this.session = session;
      this.index = index;
      this.id = id;
      this.acquirePermits = acquirePermits;
      this.expire = expire;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Waiter waiter = (Waiter) o;
      return session.equals(waiter.session)
          && index == waiter.index
          && id == waiter.id
          && acquirePermits == waiter.acquirePermits;
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
