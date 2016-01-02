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
package io.atomix.messaging;

import io.atomix.testing.AbstractCopycatTest;
import io.atomix.messaging.state.TaskQueueState;
import io.atomix.resource.ResourceStateMachine;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Distributed task queue test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class DistributedTaskQueueTest extends AbstractCopycatTest {

  @Override
  protected ResourceStateMachine createStateMachine() {
    return new TaskQueueState();
  }

  /**
   * Tests submitting and processing tasks.
   */
  public void testSubmitAsync() throws Throwable {
    createServers(3);

    DistributedTaskQueue<String> worker1 = new DistributedTaskQueue<String>(createClient()).async();
    DistributedTaskQueue<String> worker2 = new DistributedTaskQueue<String>(createClient()).async();
    DistributedTaskQueue<String> queue = new DistributedTaskQueue<String>(createClient()).async();

    Set<String> tasks = new HashSet<>(Arrays.asList("foo", "bar", "baz"));
    Set<String> completed = new ConcurrentSkipListSet<>();
    worker1.consumer(task -> {
      threadAssertTrue(tasks.contains(task));
      threadAssertFalse(completed.contains(task));
      completed.add(task);
      resume();
    }).thenRun(this::resume);
    worker2.consumer(task -> {
      threadAssertTrue(tasks.contains(task));
      threadAssertFalse(completed.contains(task));
      completed.add(task);
      resume();
    }).thenRun(this::resume);

    await(5000, 2);

    queue.submit("foo");
    queue.submit("bar");
    queue.submit("baz");
    await(5000, 3);
  }

  /**
   * Tests submitting and processing tasks and awaiting the acknowledgement.
   */
  public void testSubmitSync() throws Throwable {
    createServers(3);

    DistributedTaskQueue<String> worker1 = new DistributedTaskQueue<String>(createClient()).sync();
    DistributedTaskQueue<String> worker2 = new DistributedTaskQueue<String>(createClient()).sync();
    DistributedTaskQueue<String> queue = new DistributedTaskQueue<String>(createClient()).sync();

    Set<String> tasks = new HashSet<>(Arrays.asList("foo", "bar", "baz"));
    Set<String> completed = new ConcurrentSkipListSet<>();
    worker1.consumer(task -> {
      threadAssertTrue(tasks.contains(task));
      threadAssertFalse(completed.contains(task));
      completed.add(task);
      resume();
    }).thenRun(this::resume);
    worker2.consumer(task -> {
      threadAssertTrue(tasks.contains(task));
      threadAssertFalse(completed.contains(task));
      completed.add(task);
      resume();
    }).thenRun(this::resume);

    await(5000, 2);

    queue.submit("foo").thenRun(this::resume);
    queue.submit("bar").thenRun(this::resume);
    queue.submit("baz").thenRun(this::resume);
    await(5000, 6);
  }

}
