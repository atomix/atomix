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
 * limitations under the License.
 */
package net.kuujo.copycat.examples.server;

import net.kuujo.catalog.server.storage.Storage;
import net.kuujo.catalyst.transport.Address;
import net.kuujo.catalyst.transport.NettyTransport;
import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.CopycatReplica;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Server example.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServerExample {

  /**
   * Starts the server.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2)
      throw new IllegalArgumentException("must supply a serverId:port and at least one remoteId:host:port triple");

    int port = Integer.valueOf(args[0]);

    Address address = new Address(InetAddress.getLocalHost().getHostName(), port);

    List<Address> members = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      String[] parts = args[i].split(":");
      members.add(new Address(parts[0], Integer.valueOf(parts[1])));
    }

    Copycat copycat = CopycatReplica.builder(address, members)
        .withTransport(new NettyTransport())
        .withStorage(Storage.builder()
          .withDirectory(System.getProperty("user.dir") + "/logs/" + UUID.randomUUID().toString())
          .build())
        .build();

    copycat.open().join();

    while (copycat.isOpen()) {
      Thread.sleep(1000);
    }
  }

}
