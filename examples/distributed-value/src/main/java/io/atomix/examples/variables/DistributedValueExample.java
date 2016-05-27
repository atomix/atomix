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
package io.atomix.examples.variables;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.variables.DistributedValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Distributed value example.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DistributedValueExample {

  /**
   * Starts the client.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2)
      throw new IllegalArgumentException("must supply a path and set of host:port tuples");

    // Parse the address to which to bind the server.
    String[] mainParts = args[1].split(":");
    Address address = new Address(mainParts[0], Integer.valueOf(mainParts[1]));

    // Build a list of all member addresses to which to connect.
    List<Address> cluster = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      String[] parts = args[i].split(":");
      cluster.add(new Address(parts[0], Integer.valueOf(parts[1])));
    }

    // Create a stateful Atomix replica. The replica communicates with other replicas in the cluster
    // to replicate state changes.
    AtomixReplica atomix = AtomixReplica.builder(address)
      .withTransport(new NettyTransport())
      .withStorage(Storage.builder()
        .withStorageLevel(StorageLevel.DISK)
        .withDirectory(args[0])
        .withMinorCompactionInterval(Duration.ofSeconds(30))
        .withMajorCompactionInterval(Duration.ofMinutes(1))
        .withMaxSegmentSize(1024 * 1024 * 8)
        .withMaxEntriesPerSegment(1024 * 8)
        .build())
      .build();

    atomix.bootstrap(cluster).join();

    atomix.<String>getValue("value").thenAccept(DistributedValueExample::recursiveSet);

    for (;;) {
      Thread.sleep(1000);
    }
  }

  /**
   * Recursively sets a value.
   */
  private static void recursiveSet(DistributedValue<String> value) {
    value.set(UUID.randomUUID().toString()).thenRun(() -> {
      value.get().thenAccept(result -> {
        value.context().schedule(Duration.ofMillis(1), () -> recursiveSet(value));
      });
    });
  }

}
