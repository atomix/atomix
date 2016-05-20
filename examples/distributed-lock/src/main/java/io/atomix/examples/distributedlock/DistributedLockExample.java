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
package io.atomix.examples.distributedlock;

import io.atomix.AtomixClient;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.concurrent.DistributedLock;

import java.util.ArrayList;
import java.util.List;

/**
 * Distributed lock example
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DistributedLockExample {

  /**
   * Starts the server.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 1)
      throw new IllegalArgumentException("must supply a set of host:port tuples");

    List<Address> cluster = new ArrayList<>();
    for (int i = 0; i < args.length; i++) {
      String[] parts = args[i].split(":");
      cluster.add(new Address(parts[0], Integer.valueOf(parts[1])));
    }

    AtomixClient client = AtomixClient.builder()
      .withTransport(new NettyTransport())
      .build();

    client.connect(cluster).get();

    DistributedLock lock = client.getLock("lock").get();

    lock.lock().thenRun(() -> {
       System.out.println("Lock acquired!");
    });

  }

}
