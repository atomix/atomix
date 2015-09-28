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
package io.atomix.collections.state;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/**
 * Distributed set state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueueState extends StateMachine {
  private final Queue<Commit<? extends QueueCommands.ValueCommand>> queue = new ArrayDeque<>();

  @Override
  protected void configure(StateMachineExecutor executor) {
    executor.register(QueueCommands.Add.class, this::add);
    executor.register(QueueCommands.Offer.class, this::offer);
    executor.register(QueueCommands.Contains.class, this::contains);
    executor.register(QueueCommands.Peek.class, this::peek);
    executor.register(QueueCommands.Poll.class, this::poll);
    executor.register(QueueCommands.Element.class, this::element);
    executor.register(QueueCommands.Remove.class, this::remove);
    executor.register(QueueCommands.Size.class, this::size);
    executor.register(QueueCommands.IsEmpty.class, this::isEmpty);
    executor.register(QueueCommands.Clear.class, this::clear);
  }

  /**
   * Handles a contains commit.
   */
  protected boolean contains(Commit<QueueCommands.Contains> commit) {
    try {
      for (Commit<? extends QueueCommands.ValueCommand> value : queue) {
        if (value.operation().value().equals(commit.operation().value()))
          return true;
      }
      return false;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an add commit.
   */
  protected boolean add(Commit<QueueCommands.Add> commit) {
    try {
      queue.add(commit);
    } catch (Exception e) {
      commit.clean();
      throw e;
    }
    return false;
  }

  /**
   * Handles an offer commit.
   */
  protected boolean offer(Commit<QueueCommands.Offer> commit) {
    try {
      queue.offer(commit);
    } catch (Exception e) {
      commit.clean();
      throw e;
    }
    return false;
  }

  /**
   * Handles a peek commit.
   */
  protected Object peek(Commit<QueueCommands.Peek> commit) {
    try {
      Commit<? extends QueueCommands.ValueCommand> value = queue.peek();
      if (value != null) {
        value.clean();
        return value.operation().value();
      }
      return null;
    } finally {
      commit.clean();
    }
  }

  /**
   * Handles a poll commit.
   */
  protected Object poll(Commit<QueueCommands.Poll> commit) {
    try {
      Commit<? extends QueueCommands.ValueCommand> value = queue.poll();
      if (value != null) {
        value.clean();
        return value.operation().value();
      }
      return null;
    } finally {
      commit.clean();
    }
  }

  /**
   * Handles an element commit.
   */
  protected Object element(Commit<QueueCommands.Element> commit) {
    try {
      Commit<? extends QueueCommands.ValueCommand> value = queue.element();
      if (value != null) {
        value.clean();
        return value.operation().value();
      }
      return null;
    } finally {
      commit.clean();
    }
  }

  /**
   * Handles a remove commit.
   */
  protected Object remove(Commit<QueueCommands.Remove> commit) {
    try {
      if (commit.operation().value() != null) {
        Iterator<Commit<? extends QueueCommands.ValueCommand>> iterator = queue.iterator();
        while (iterator.hasNext()) {
          Commit<? extends QueueCommands.ValueCommand> value = iterator.next();
          if (value.operation().value().equals(commit.operation().value())) {
            iterator.remove();
            value.clean();
            return true;
          }
        }
        return false;
      } else {
        Commit<? extends QueueCommands.ValueCommand> value = queue.remove();
        if (value != null) {
          value.clean();
          return value.operation().value();
        }
        return null;
      }
    } finally {
      commit.clean();
    }
  }

  /**
   * Handles a count commit.
   */
  protected int size(Commit<QueueCommands.Size> commit) {
    try {
      return queue.size();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an is empty commit.
   */
  protected boolean isEmpty(Commit<QueueCommands.IsEmpty> commit) {
    try {
      return queue.isEmpty();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a clear commit.
   */
  protected void clear(Commit<QueueCommands.Clear> commit) {
    try {
      Iterator<Commit<? extends QueueCommands.ValueCommand>> iterator = queue.iterator();
      while (iterator.hasNext()) {
        Commit<? extends QueueCommands.ValueCommand> value = iterator.next();
        value.clean();
        iterator.remove();
      }
    } finally {
      commit.clean();
    }
  }

}
