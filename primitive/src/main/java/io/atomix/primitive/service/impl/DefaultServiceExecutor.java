/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive.service.impl;

import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Default operation executor.
 */
public class DefaultServiceExecutor implements ServiceExecutor {
  private final Serializer serializer;
  private final ServiceContext context;
  private final Logger log;
  private final Queue<Runnable> tasks = new LinkedList<>();
  private final List<ScheduledTask> scheduledTasks = new ArrayList<>();
  private final List<ScheduledTask> complete = new ArrayList<>();
  private final Map<String, Function<Commit<byte[]>, byte[]>> operations = new HashMap<>();
  private OperationType operationType;
  private long timestamp;

  public DefaultServiceExecutor(ServiceContext context, Serializer serializer) {
    this.serializer = checkNotNull(serializer);
    this.context = checkNotNull(context);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveService.class)
        .addValue(context.serviceId())
        .add("type", context.serviceType())
        .add("name", context.serviceName())
        .build());
  }

  /**
   * Encodes the given object using the configured {@link #serializer}.
   *
   * @param object the object to encode
   * @param <T>    the object type
   * @return the encoded bytes
   */
  protected <T> byte[] encode(T object) {
    return object != null ? serializer.encode(object) : null;
  }

  /**
   * Decodes the given object using the configured {@link #serializer}.
   *
   * @param bytes the bytes to decode
   * @param <T>   the object type
   * @return the decoded object
   */
  protected <T> T decode(byte[] bytes) {
    return bytes != null ? serializer.decode(bytes) : null;
  }

  @Override
  public void tick(WallClockTimestamp timestamp) {
    long unixTimestamp = timestamp.unixTimestamp();
    this.operationType = OperationType.COMMAND;
    if (!scheduledTasks.isEmpty()) {
      // Iterate through scheduled tasks until we reach a task that has not met its scheduled time.
      // The tasks list is sorted by time on insertion.
      Iterator<ScheduledTask> iterator = scheduledTasks.iterator();
      while (iterator.hasNext()) {
        ScheduledTask task = iterator.next();
        if (task.isRunnable(unixTimestamp)) {
          this.timestamp = task.time;
          this.operationType = OperationType.COMMAND;
          log.trace("Executing scheduled task {}", task);
          task.execute();
          complete.add(task);
          iterator.remove();
        } else {
          break;
        }
      }

      // Iterate through tasks that were completed and reschedule them.
      for (ScheduledTask task : complete) {
        task.reschedule(this.timestamp);
      }
      complete.clear();
    }
  }

  /**
   * Checks that the current operation is of the given type.
   *
   * @param type    the operation type
   * @param message the message to print if the current operation does not match the given type
   */
  private void checkOperation(OperationType type, String message) {
    checkState(operationType == type, message);
  }

  @Override
  public void handle(OperationId operationId, Function<Commit<byte[]>, byte[]> callback) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    operations.put(operationId.id(), callback);
    log.trace("Registered operation callback {}", operationId);
  }

  @Override
  public <R> void register(OperationId operationId, Supplier<R> callback) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    handle(operationId, commit -> encode(callback.get()));
  }

  @Override
  public <T> void register(OperationId operationId, Consumer<Commit<T>> callback) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    handle(operationId, commit -> {
      callback.accept(commit.map(this::decode));
      return null;
    });
  }

  @Override
  public <T, R> void register(OperationId operationId, Function<Commit<T>, R> callback) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    handle(operationId, commit -> encode(callback.apply(commit.map(this::decode))));
  }

  @Override
  public byte[] apply(Commit<byte[]> commit) {
    log.trace("Executing {}", commit);

    this.operationType = commit.operation().type();
    this.timestamp = commit.wallClockTime().unixTimestamp();

    // Look up the registered callback for the operation.
    Function<Commit<byte[]>, byte[]> operation = operations.get(commit.operation().id());

    if (operation == null) {
      throw new IllegalStateException("Unknown state machine operation: " + commit.operation());
    } else {
      // Execute the operation. If the operation return value is a Future, await the result,
      // otherwise immediately complete the execution future.
      try {
        return operation.apply(commit);
      } catch (Exception e) {
        log.warn("State machine operation failed: {}", e.getMessage());
        throw new PrimitiveException.ServiceException(e);
      } finally {
        runTasks();
      }
    }
  }

  /**
   * Executes tasks after an operation.
   */
  private void runTasks() {
    // Execute any tasks that were queue during execution of the command.
    if (!tasks.isEmpty()) {
      for (Runnable task : tasks) {
        log.trace("Executing task {}", task);
        task.run();
      }
      tasks.clear();
    }
  }

  @Override
  public void execute(Runnable callback) {
    checkOperation(OperationType.COMMAND, "callbacks can only be scheduled during command execution");
    checkNotNull(callback, "callback cannot be null");
    tasks.add(callback);
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable callback) {
    checkOperation(OperationType.COMMAND, "callbacks can only be scheduled during command execution");
    checkArgument(!delay.isNegative(), "delay cannot be negative");
    checkNotNull(callback, "callback cannot be null");
    log.trace("Scheduled callback {} with delay {}", callback, delay);
    return new ScheduledTask(callback, delay.toMillis()).schedule();
  }

  @Override
  public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
    checkOperation(OperationType.COMMAND, "callbacks can only be scheduled during command execution");
    checkArgument(!initialDelay.isNegative(), "initialDelay cannot be negative");
    checkArgument(!interval.isNegative(), "interval cannot be negative");
    checkNotNull(callback, "callback cannot be null");
    log.trace("Scheduled repeating callback {} with initial delay {} and interval {}", callback, initialDelay, interval);
    return new ScheduledTask(callback, initialDelay.toMillis(), interval.toMillis()).schedule();
  }

  /**
   * Scheduled task.
   */
  private class ScheduledTask implements Scheduled {
    private final long interval;
    private final Runnable callback;
    private long time;

    private ScheduledTask(Runnable callback, long delay) {
      this(callback, delay, 0);
    }

    private ScheduledTask(Runnable callback, long delay, long interval) {
      this.interval = interval;
      this.callback = callback;
      this.time = timestamp + delay;
    }

    /**
     * Schedules the task.
     */
    private Scheduled schedule() {
      // Perform binary search to insert the task at the appropriate position in the tasks list.
      if (scheduledTasks.isEmpty()) {
        scheduledTasks.add(this);
      } else {
        int l = 0;
        int u = scheduledTasks.size() - 1;
        int i;
        while (true) {
          i = (u + l) / 2;
          long t = scheduledTasks.get(i).time;
          if (t == time) {
            scheduledTasks.add(i, this);
            return this;
          } else if (t < time) {
            l = i + 1;
            if (l > u) {
              scheduledTasks.add(i + 1, this);
              return this;
            }
          } else {
            u = i - 1;
            if (l > u) {
              scheduledTasks.add(i, this);
              return this;
            }
          }
        }
      }
      return this;
    }

    /**
     * Reschedules the task.
     */
    private void reschedule(long timestamp) {
      if (interval > 0) {
        time = timestamp + interval;
        schedule();
      }
    }

    /**
     * Returns a boolean value indicating whether the task delay has been met.
     */
    private boolean isRunnable(long timestamp) {
      return timestamp > time;
    }

    /**
     * Executes the task.
     */
    private synchronized void execute() {
      try {
        callback.run();
      } catch (Exception e) {
        log.error("An exception occurred in a scheduled task", e);
      }
    }

    @Override
    public synchronized void cancel() {
      scheduledTasks.remove(this);
    }
  }
}
