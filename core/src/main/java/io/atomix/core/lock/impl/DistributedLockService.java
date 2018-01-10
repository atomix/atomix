/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.lock.impl;

import io.atomix.core.lock.impl.DistributedLockOperations.Lock;
import io.atomix.core.lock.impl.DistributedLockOperations.Unlock;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.atomix.core.lock.impl.DistributedLockOperations.LOCK;
import static io.atomix.core.lock.impl.DistributedLockOperations.UNLOCK;

/**
 * Raft atomic value service.
 */
public class DistributedLockService extends AbstractPrimitiveService {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(DistributedLockOperations.NAMESPACE)
      .register(DistributedLockEvents.NAMESPACE)
      .register(LockHolder.class)
      .build());

  private LockHolder lock;
  private Queue<LockHolder> queue = new ArrayDeque<>();
  private final Map<Long, Scheduled> timers = new HashMap<>();

  @Override
  protected void configure(ServiceExecutor executor) {
    executor.register(LOCK, SERIALIZER::decode, this::lock);
    executor.register(UNLOCK, SERIALIZER::decode, this::unlock);
  }

  @Override
  public void backup(BufferOutput<?> writer) {
    writer.writeObject(lock, SERIALIZER::encode);
    writer.writeObject(queue, SERIALIZER::encode);
  }

  @Override
  public void restore(BufferInput<?> reader) {
    lock = reader.readObject(SERIALIZER::decode);
    queue = reader.readObject(SERIALIZER::decode);
    timers.values().forEach(Scheduled::cancel);
    timers.clear();
    for (LockHolder holder : queue) {
      if (holder.expire > 0) {
        timers.put(holder.index, getScheduler().schedule(Duration.ofMillis(holder.expire - getWallClock().getTime().unixTimestamp()), () -> {
          timers.remove(holder.index);
          queue.remove(holder);
          Session session = getSessions().getSession(holder.session);
          if (session != null && session.getState().active()) {
            session.publish(DistributedLockEvents.FAIL, SERIALIZER::encode, new LockEvent(holder.id, holder.index));
          }
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

  /**
   * Applies a lock commit.
   */
  protected void lock(Commit<Lock> commit) {
    if (lock == null) {
      lock = new LockHolder(
          commit.value().id(),
          commit.index(),
          commit.session().sessionId().id(),
          0);
      commit.session().publish(DistributedLockEvents.LOCK, SERIALIZER::encode, new LockEvent(commit.value().id(), commit.index()));
    } else if (commit.value().timeout() == 0) {
      commit.session().publish(DistributedLockEvents.FAIL, SERIALIZER::encode, new LockEvent(commit.value().id(), commit.index()));
    } else if (commit.value().timeout() > 0) {
      LockHolder holder = new LockHolder(
          commit.value().id(),
          commit.index(),
          commit.session().sessionId().id(),
          getWallClock().getTime().unixTimestamp() + commit.value().timeout());
      queue.add(holder);
      timers.put(commit.index(), getScheduler().schedule(Duration.ofMillis(commit.value().timeout()), () -> {
        timers.remove(commit.index());
        queue.remove(holder);
        if (commit.session().getState().active()) {
          commit.session().publish(DistributedLockEvents.FAIL, SERIALIZER::encode, new LockEvent(commit.value().id(), commit.index()));
        }
      }));
    } else {
      LockHolder holder = new LockHolder(
          commit.value().id(),
          commit.index(),
          commit.session().sessionId().id(),
          0);
      queue.add(holder);
    }
  }

  /**
   * Applies an unlock commit.
   */
  protected void unlock(Commit<Unlock> commit) {
    if (lock != null) {
      if (lock.session != commit.session().sessionId().id()) {
        return;
      }

      lock = queue.poll();
      while (lock != null) {
        Scheduled timer = timers.remove(lock.index);
        if (timer != null) {
          timer.cancel();
        }

        Session session = getSessions().getSession(lock.session);
        if (session == null || session.getState() == Session.State.EXPIRED || session.getState() == Session.State.CLOSED) {
          lock = queue.poll();
        } else {
          session.publish(DistributedLockEvents.LOCK, SERIALIZER::encode, new LockEvent(lock.id, commit.index()));
          break;
        }
      }
    }
  }

  private void releaseSession(Session session) {
    if (lock.session == session.sessionId().id()) {
      lock = queue.poll();
      while (lock != null) {
        if (lock.session == session.sessionId().id()) {
          lock = queue.poll();
        } else {
          Scheduled timer = timers.remove(lock.index);
          if (timer != null) {
            timer.cancel();
          }

          Session lockSession = getSessions().getSession(lock.session);
          if (lockSession == null || lockSession.getState() == Session.State.EXPIRED || lockSession.getState() == Session.State.CLOSED) {
            lock = queue.poll();
          } else {
            lockSession.publish(DistributedLockEvents.LOCK, SERIALIZER::encode, new LockEvent(lock.id, lock.index));
            break;
          }
        }
      }
    }
  }

  private class LockHolder {
    private final int id;
    private final long index;
    private final long session;
    private final long expire;

    public LockHolder(int id, long index, long session, long expire) {
      this.id = id;
      this.index = index;
      this.session = session;
      this.expire = expire;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("id", id)
          .add("index", index)
          .add("session", session)
          .add("expire", expire)
          .toString();
    }
  }
}