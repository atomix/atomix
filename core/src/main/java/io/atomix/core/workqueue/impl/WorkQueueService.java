// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.workqueue.impl;

import io.atomix.core.workqueue.Task;
import io.atomix.core.workqueue.WorkQueueStats;
import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

import java.util.Collection;

/**
 * Work queue service.
 */
public interface WorkQueueService {

  /**
   * Adds a collection of tasks to the work queue.
   *
   * @param items collection of task items
   */
  @Command
  void add(Collection<byte[]> items);

  /**
   * Picks up multiple tasks from the work queue to work on.
   *
   * @param maxItems maximum number of items to take from the queue. The actual number of tasks returned
   *                 can be at the max this number
   * @return an empty collection if there are no unassigned tasks in the work queue
   */
  @Command
  Collection<Task<byte[]>> take(int maxItems);

  /**
   * Completes a collection of tasks.
   *
   * @param taskIds ids of tasks to complete
   */
  @Command
  void complete(Collection<String> taskIds);

  /**
   * Returns work queue statistics.
   *
   * @return work queue stats
   */
  @Query
  WorkQueueStats stats();

  /**
   * Registers the current session as a task processor.
   */
  @Command
  void register();

  /**
   * Unregisters the current session as a task processor.
   */
  @Command
  void unregister();

  /**
   * Removes all pending tasks from the queue.
   */
  @Command
  void clear();

}
