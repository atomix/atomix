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

import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.messaging.DistributedTaskQueue;
import io.atomix.resource.ResourceStateMachine;
import io.atomix.resource.ResourceType;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Task queue state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class TaskQueueState extends ResourceStateMachine implements SessionListener {
  private static final int BATCH_SIZE = 8;
  private final Map<Long, Commit<TaskQueueCommands.Subscribe>> workers = new HashMap<>();
  private final LinkedBlockingDeque<Commit<TaskQueueCommands.Submit>> taskQueue = new LinkedBlockingDeque<>();
  private final Map<Long, List<Commit<TaskQueueCommands.Submit>>> processing = new HashMap<>();

  public TaskQueueState() {
    super(new ResourceType(DistributedTaskQueue.class));
  }

  @Override
  public void close(Session session) {
    // Remove and close the subscription.
    Commit<TaskQueueCommands.Subscribe> commit = workers.remove(session.id());
    if (commit != null)
      commit.close();

    // Remove the session from the worker queue.
    workers.remove(session.id());

    List<Commit<TaskQueueCommands.Submit>> tasks = processing.remove(session.id());
    if (tasks != null) {
      Collections.reverse(tasks);
      for (Commit<TaskQueueCommands.Submit> task : tasks) {
        Session worker = selectWorker();
        if (worker != null) {
          worker.publish("process", task.operation().task());
        } else {
          taskQueue.addFirst(task);
        }
      }
    }
  }

  /**
   * Selects a worker to which to publish a task.
   */
  private Session selectWorker() {
    int count = -1;
    long worker = -1;
    for (Map.Entry<Long, List<Commit<TaskQueueCommands.Submit>>> entry : processing.entrySet()) {
      List<Commit<TaskQueueCommands.Submit>> tasks = entry.getValue();
      if (tasks.size() < BATCH_SIZE && count == -1 || tasks.size() < count) {
        count = entry.getValue().size();
        worker = entry.getKey();
      }
    }
    return worker != -1 ? workers.get(worker).session() : null;
  }

  /**
   * Handles a subscribe commit.
   */
  public void subscribe(Commit<TaskQueueCommands.Subscribe> commit) {
    workers.put(commit.session().id(), commit);
    List<Commit<TaskQueueCommands.Submit>> tasks = new ArrayList<>(BATCH_SIZE);
    processing.put(commit.session().id(), tasks);

    Commit<TaskQueueCommands.Submit> task = taskQueue.poll();
    while (task != null && tasks.size() < BATCH_SIZE) {
      tasks.add(task);
      commit.session().publish("process", task.operation().task());
      task = taskQueue.poll();
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
      Session worker = selectWorker();
      if (worker != null) {
        List<Commit<TaskQueueCommands.Submit>> tasks = processing.get(worker.id());
        tasks.add(commit);
        worker.publish("process", commit.operation().task());
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
      List<Commit<TaskQueueCommands.Submit>> tasks = processing.get(commit.session().id());

      // If no known task was being processed, throw an exception.
      if (tasks == null || tasks.isEmpty()) {
        throw new IllegalStateException("unknown task");
      }

      Commit<TaskQueueCommands.Submit> task = tasks.remove(0);

      // Send an ack message to the session that submitted the task.
      if (task.operation().ack() && task.session().state() == Session.State.OPEN) {
        task.session().publish("ack", task.operation().id());
      }

      // Close the task commit to allow it to be garbage collected.
      task.close();

      // Get and send the next task in the queue.
      Commit<TaskQueueCommands.Submit> next = taskQueue.poll();
      if (next != null) {
        tasks.add(next);
        return next.operation().task();
      }
      return null;
    } finally {
      commit.close();
    }
  }

}
