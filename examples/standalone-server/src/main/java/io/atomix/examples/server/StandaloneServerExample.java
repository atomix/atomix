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
package io.atomix.examples.server;

import io.atomix.AtomixServer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.server.storage.Storage;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Server example.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StandaloneServerExample {

  /**
   * Starts the server.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2)
      throw new IllegalArgumentException("must supply a local port and at least one remote host:port tuple");

    String[] mainParts = args[1].split(":");
    Address serverAddress = new Address(mainParts[0], Integer.valueOf(mainParts[1]));
    Address clientAddress = new Address(mainParts[0], Integer.valueOf(mainParts[2]));

    List<Address> members = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      String[] parts = args[i].split(":");
      members.add(new Address(parts[0], Integer.valueOf(parts[1])));
    }

    AtomixServer server = AtomixServer.builder(clientAddress, serverAddress, members)
        .withTransport(new NettyTransport())
        .withStorage(Storage.builder()
          .withDirectory(System.getProperty("user.dir") + "/logs/" + mainParts[1])
          .withMaxEntriesPerSegment(16)
          .build())
        .build();

    server.open().join();

    while (server.isOpen()) {
      Thread.sleep(1000);
    }
  }

}
