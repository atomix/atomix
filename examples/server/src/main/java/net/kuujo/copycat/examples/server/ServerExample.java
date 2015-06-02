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
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.NettyCluster;
import net.kuujo.copycat.cluster.NettyMember;
import net.kuujo.copycat.raft.log.Log;
import net.kuujo.copycat.raft.log.StorageLevel;

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
      throw new IllegalArgumentException("must supply a server ID and at least one ID:host:port triple");

    int serverId = Integer.valueOf(args[0]);

    NettyCluster.Builder builder = NettyCluster.builder()
      .withMemberId(serverId)
      .withMemberType(Member.Type.ACTIVE);

    for (int i = 1; i < args.length; i++) {
      String[] parts = args[i].split(":");
      builder.addMember(NettyMember.builder()
        .withId(Integer.valueOf(parts[0]))
        .withHost(parts[1])
        .withPort(Integer.valueOf(parts[2]))
        .build());
    }

    Copycat copycat = Copycat.builder()
      .withCluster(builder.build())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();

    copycat.open().join();

    while (copycat.isOpen()) {
      Thread.sleep(1000);
    }
  }

}
