// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.workqueue;

import com.google.common.base.MoreObjects;

import java.util.function.Function;

/**
 * {@link AsyncWorkQueue} task.
 *
 * @param <E> task payload type.
 */
public class Task<E> {
  private final E payload;
  private final String taskId;

  private Task() {
    payload = null;
    taskId = null;
  }

  /**
   * Constructs a new task instance.
   *
   * @param taskId  task identifier
   * @param payload task payload
   */
  public Task(String taskId, E payload) {
    this.taskId = taskId;
    this.payload = payload;
  }

  /**
   * Returns the task identifier.
   *
   * @return task id
   */
  public String taskId() {
    return taskId;
  }

  /**
   * Returns the task payload.
   *
   * @return task payload
   */
  public E payload() {
    return payload;
  }

  /**
   * Maps task from one payload type to another.
   *
   * @param <F>    future type
   * @param mapper type mapper.
   * @return mapped task.
   */
  public <F> Task<F> map(Function<E, F> mapper) {
    return new Task<>(taskId, mapper.apply(payload));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("taskId", taskId)
        .add("payload", payload)
        .toString();
  }
}
