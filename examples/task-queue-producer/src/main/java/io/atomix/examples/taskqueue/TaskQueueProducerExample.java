/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.examples.taskqueue;

import io.atomix.Atomix;
import io.atomix.AtomixClient;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.messaging.DistributedTaskQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Task queue producer example.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class TaskQueueProducerExample {

  /**
   * Starts the server.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 1)
      throw new IllegalArgumentException("must supply a set of host:port tuples");

    // Build a list of all member addresses to which to connect.
    List<Address> members = new ArrayList<>();
    for (String arg : args) {
      String[] parts = arg.split(":");
      members.add(new Address(parts[0], Integer.valueOf(parts[1])));
    }

    // Create a stateful Atomix replica. The replica communicates with other replicas in the cluster
    // to replicate state changes.
    Atomix atomix = AtomixClient.builder(members)
      .withTransport(new NettyTransport())
      .build();

    // Open the client. Once this operation completes resources can be created and managed.
    atomix.open().join();

    // Create a task queue resource.
    @SuppressWarnings("unchecked")
    DistributedTaskQueue<String> queue = atomix.create("queue", DistributedTaskQueue.class).get().async();

    // Register a callback to be called when a message is received.
    for (int i = 0; i < 100; i++) {
      submitTasks(queue);
    }

    // Block while the replica is open.
    while (atomix.isOpen()) {
      Thread.sleep(1000);
    }
  }

  /**
   * Recursively submits tasks to the queue.
   */
  private static void submitTasks(DistributedTaskQueue<String> queue) {
    queue.submit(UUID.randomUUID().toString()).whenComplete((result, error) -> submitTasks(queue));
  }

}
