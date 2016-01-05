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

import io.atomix.resource.ResourceType;
import io.atomix.testing.AbstractCopycatTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Distributed topic test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class DistributedTopicTest extends AbstractCopycatTest<DistributedTopic> {

  @Override
  protected Class<? super DistributedTopic> type() {
    return DistributedTopic.class;
  }

  /**
   * Tests publishing and receiving messages asynchronously.
   */
  public void testPublishAsync() throws Throwable {
    createServers(3);

    DistributedTopic<String> subscriber1 = createResource().async();
    DistributedTopic<String> subscriber2 = createResource().async();
    DistributedTopic<String> queue = createResource().async();

    Set<String> messages = new HashSet<>(Arrays.asList("foo", "bar", "baz"));
    subscriber1.subscribe(message -> {
      threadAssertTrue(messages.contains(message));
      resume();
    }).thenRun(this::resume);
    subscriber2.subscribe(message -> {
      threadAssertTrue(messages.contains(message));
      resume();
    }).thenRun(this::resume);

    await(5000, 2);

    queue.publish("foo");
    queue.publish("bar");
    queue.publish("baz");
    await(5000, 6);
  }

  /**
   * Tests publishing and receiving messages synchronously.
   */
  public void testPublishSync() throws Throwable {
    createServers(3);

    DistributedTopic<String> subscriber1 = createResource().sync();
    DistributedTopic<String> subscriber2 = createResource().sync();
    DistributedTopic<String> queue = createResource().sync();

    Set<String> messages = new HashSet<>(Arrays.asList("foo", "bar", "baz"));
    subscriber1.subscribe(message -> {
      threadAssertTrue(messages.contains(message));
      resume();
    }).thenRun(this::resume);
    subscriber2.subscribe(message -> {
      threadAssertTrue(messages.contains(message));
      resume();
    }).thenRun(this::resume);

    await(5000, 2);

    queue.publish("foo").thenRun(this::resume);
    queue.publish("bar").thenRun(this::resume);
    queue.publish("baz").thenRun(this::resume);
    await(5000, 9);
  }

}
