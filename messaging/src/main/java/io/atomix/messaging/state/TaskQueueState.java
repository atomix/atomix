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
 * limitations under the License
 */
package io.atomix.messaging.state;

import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceStateMachine;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Task queue state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class TaskQueueState extends ResourceStateMachine implements SessionListener {
  private final Map<Long, Commit<TaskQueueCommands.Subscribe>> workers = new HashMap<>();
  private final Queue<ServerSession> workerQueue = new ArrayDeque<>();
  private final LinkedBlockingDeque<Commit<TaskQueueCommands.Submit>> taskQueue = new LinkedBlockingDeque<>();
  private final Map<Long, Commit<TaskQueueCommands.Submit>> processing = new HashMap<>();

  public TaskQueueState(Resource.Config config) {
    super(config);
  }

  @Override
  protected void registerTypes(SerializerRegistry registry) {
    new TaskQueueCommands.TypeResolver().resolve(registry);
  }

  @Override
  public void close(ServerSession session) {
    // Remove and close the subscription.
    Commit<TaskQueueCommands.Subscribe> commit = workers.remove(session.id());
    if (commit != null)
      commit.close();

    // Remove the session from the worker queue.
    workerQueue.remove(session);

    Commit<TaskQueueCommands.Submit> task = processing.remove(session.id());
    if (task != null) {
      ServerSession next = workerQueue.poll();
      if (next != null) {
        next.publish("process", task.operation().task());
      } else {
        taskQueue.addFirst(task);
      }
    }
  }

  /**
   * Handles a subscribe commit.
   */
  public void subscribe(Commit<TaskQueueCommands.Subscribe> commit) {
    workers.put(commit.session().id(), commit);

    Commit<TaskQueueCommands.Submit> task = taskQueue.poll();
    if (task != null) {
      processing.put(commit.session().id(), task);
      commit.session().publish("process", task.operation().task());
    } else {
      workerQueue.add(commit.session());
    }
  }

  /**
   * Handles an unsubscribe commit.
   */
  public void unsubscribe(Commit<TaskQueueCommands.Unsubscribe> commit) {
    close(commit.session());
  }

  /**
   * Handles a task submission.
   */
  public void submit(Commit<TaskQueueCommands.Submit> commit) {
    try {
      ServerSession session = workerQueue.poll();
      if (session != null) {
        session.publish("process", commit.operation().task());
        processing.put(session.id(), commit);
      } else {
        taskQueue.add(commit);
      }
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  /**
   * Handles a task acknowledgement.
   */
  public Object ack(Commit<TaskQueueCommands.Ack> commit) {
    try {
      // Remove the task being processed by the given commit.
      Commit<TaskQueueCommands.Submit> acked = processing.remove(commit.session().id());

      // If no known task was being processed, throw an exception.
      if (acked == null) {
        throw new IllegalStateException("unknown task");
      }

      // Send an ack message to the session that submitted the task.
      if (acked.operation().ack() && acked.session().state() == ServerSession.State.OPEN) {
        acked.session().publish("ack", acked.operation().id());
      }

      // Close the task commit to allow it to be garbage collected.
      acked.close();

      // Get and send the next task in the queue.
      Commit<TaskQueueCommands.Submit> next = taskQueue.poll();
      if (next != null) {
        processing.put(commit.session().id(), next);
        return next.operation().task();
      } else {
        workerQueue.add(commit.session());
        return null;
      }
    } finally {
      commit.close();
    }
  }

  @Override
  public void delete() {
    workers.values().forEach(Commit::close);
    taskQueue.forEach(Commit::close);
    processing.values().forEach(Commit::close);
  }

}
