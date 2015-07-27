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

import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.CopycatServer;
import net.kuujo.copycat.transport.NettyTransport;
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.StorageLevel;

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

    String[] parts = args[0].split(":");
    int serverId = Integer.valueOf(parts[0]);
    int port = Integer.valueOf(parts[1]);

    Members.Builder builder = Members.builder()
      .addMember(Member.builder()
        .withId(serverId)
        .withHost("localhost")
        .withPort(port)
        .build());

    for (int i = 1; i < args.length; i++) {
      parts = args[i].split(":");
      builder.addMember(Member.builder()
          .withId(Integer.valueOf(parts[0]))
          .withHost(parts[1])
          .withPort(Integer.valueOf(parts[2]))
          .build());
    }

    Copycat copycat = CopycatServer.builder()
      .withTransport(NettyTransport.builder().build())
      .withMemberId(serverId)
      .withMembers(builder.build())
      .withLog(Log.builder().withStorageLevel(StorageLevel.MEMORY).build())
      .build();

    copycat.open().join();

    while (copycat.isOpen()) {
      Thread.sleep(1000);
    }
  }

}
