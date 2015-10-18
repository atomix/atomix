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
package io.atomix.examples.atomic;

import io.atomix.Atomix;
import io.atomix.AtomixClient;
import io.atomix.atomic.DistributedAtomicValue;
import io.atomix.catalyst.transport.Address;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Atomic value example.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AtomicValueExample {

  /**
   * Starts the client.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 1)
      throw new IllegalArgumentException("must supply at least one server host:port");

    List<Address> members = new ArrayList<>();
    for (int i = 0; i < args.length; i++) {
      String[] parts = args[i].split(":");
      members.add(new Address(parts[0], Integer.valueOf(parts[1])));
    }

    Atomix atomix = AtomixClient.builder(members).build();

    atomix.open().join();

    atomix.<DistributedAtomicValue<String>>create("atomic", DistributedAtomicValue.class).thenAccept(AtomicValueExample::recursiveSet);

    while (atomix.isOpen()) {
      Thread.sleep(1000);
    }
  }

  /**
   * Recursively sets a value.
   */
  private static void recursiveSet(DistributedAtomicValue<String> value) {
    value.set(UUID.randomUUID().toString()).thenRun(() -> {
      value.get().thenAccept(result -> {
        System.out.println("Value is: " + result);
        value.context().schedule(Duration.ofSeconds(1), () -> recursiveSet(value));
      });
    });
  }

}
