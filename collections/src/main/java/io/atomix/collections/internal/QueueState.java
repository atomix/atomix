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
package io.atomix.collections.internal;

import io.atomix.copycat.server.Commit;
import io.atomix.resource.ResourceStateMachine;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Properties;
import java.util.Queue;

import static io.atomix.collections.DistributedQueue.Events;
import static io.atomix.collections.DistributedQueue.ValueEvent;

/**
 * Distributed set state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueueState extends ResourceStateMachine {
  private final Queue<Commit<? extends QueueCommands.ValueCommand>> queue = new ArrayDeque<>();

  public QueueState(Properties properties) {
    super(properties);
  }

  /**
   * Handles a contains commit.
   */
  public boolean contains(Commit<QueueCommands.Contains> commit) {
    try {
      for (Commit<? extends QueueCommands.ValueCommand> value : queue) {
        if (value.operation().value().equals(commit.operation().value()))
          return true;
      }
      return false;
    } finally {
      commit.release();
    }
  }

  /**
   * Handles an add commit.
   */
  public boolean add(Commit<QueueCommands.Add> commit) {
    try {
      queue.add(commit);
      notify(new ValueEvent<>(Events.ADD, commit.command().value()));
    } catch (Exception e) {
      commit.release();
      throw e;
    }
    return true;
  }

  /**
   * Handles an offer commit.
   */
  public boolean offer(Commit<QueueCommands.Offer> commit) {
    try {
      if (queue.offer(commit)) {
        notify(new ValueEvent<>(Events.ADD, commit.command().value()));
        return true;
      } else {
        commit.release();
        return false;
      }
    } catch (Exception e) {
      commit.release();
      throw e;
    }
  }

  /**
   * Handles a peek commit.
   */
  public Object peek(Commit<QueueCommands.Peek> commit) {
    try {
      Commit<? extends QueueCommands.ValueCommand> value = queue.peek();
      if (value != null) {
        return value.operation().value();
      }
      return null;
    } finally {
      commit.release();
    }
  }

  /**
   * Handles a poll commit.
   */
  public Object poll(Commit<QueueCommands.Poll> commit) {
    try {
      Commit<? extends QueueCommands.ValueCommand> value = queue.poll();
      if (value != null) {
        try {
          notify(new ValueEvent<>(Events.REMOVE, value.command().value()));
          return value.operation().value();
        } finally {
          value.release();
        }
      }
      return null;
    } finally {
      commit.release();
    }
  }

  /**
   * Handles an element commit.
   */
  public Object element(Commit<QueueCommands.Element> commit) {
    try {
      Commit<? extends QueueCommands.ValueCommand> value = queue.element();
      if (value != null) {
        try {
          return value.operation().value();
        } finally {
          value.release();
        }
      }
      return null;
    } finally {
      commit.release();
    }
  }

  /**
   * Handles a remove commit.
   */
  public Object remove(Commit<QueueCommands.Remove> commit) {
    try {
      if (commit.operation().value() != null) {
        Iterator<Commit<? extends QueueCommands.ValueCommand>> iterator = queue.iterator();
        while (iterator.hasNext()) {
          Commit<? extends QueueCommands.ValueCommand> value = iterator.next();
          if (value.operation().value().equals(commit.operation().value())) {
            iterator.remove();
            notify(new ValueEvent<>(Events.REMOVE, value.command().value()));
            value.release();
            return true;
          }
        }
        return false;
      } else {
        Commit<? extends QueueCommands.ValueCommand> value = queue.remove();
        if (value != null) {
          try {
            notify(new ValueEvent<>(Events.REMOVE, value.command().value()));
            return value.operation().value();
          } finally {
            value.release();
          }
        }
        return null;
      }
    } finally {
      commit.release();
    }
  }

  /**
   * Handles a count commit.
   */
  public int size(Commit<QueueCommands.Size> commit) {
    try {
      return queue.size();
    } finally {
      commit.release();
    }
  }

  /**
   * Handles an is empty commit.
   */
  public boolean isEmpty(Commit<QueueCommands.IsEmpty> commit) {
    try {
      return queue.isEmpty();
    } finally {
      commit.release();
    }
  }

  /**
   * Handles a clear commit.
   */
  public void clear(Commit<QueueCommands.Clear> commit) {
    try {
      delete();
    } finally {
      commit.release();
    }
  }

  @Override
  public void delete() {
    Iterator<Commit<? extends QueueCommands.ValueCommand>> iterator = queue.iterator();
    while (iterator.hasNext()) {
      Commit<? extends QueueCommands.ValueCommand> value = iterator.next();
      value.release();
      iterator.remove();
    }
  }

}
