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
package net.kuujo.copycat.cluster;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Netty cluster builder test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class NettyClusterBuilderTest {

  /**
   * Tests building a Netty cluster.
   */
  public void testBuildCluster() {
    Cluster cluster = NettyCluster.builder()
      .withLocalMember(NettyLocalMember.builder()
        .withId(1)
        .withType(Member.Type.ACTIVE)
        .withHost("localhost")
        .withPort(8080)
        .build())
      .addRemoteMember(NettyRemoteMember.builder()
        .withId(2)
        .withType(Member.Type.ACTIVE)
        .withHost("localhost")
        .withPort(8081)
        .build())
      .addRemoteMember(NettyRemoteMember.builder()
        .withId(3)
        .withType(Member.Type.ACTIVE)
        .withHost("localhost")
        .withPort(8082)
        .build())
      .build();

    assertEquals(cluster.member().id(), 1);
    assertEquals(cluster.members().size(), 3);
    assertEquals(cluster.member(1).id(), 1);
    assertEquals(cluster.member(2).id(), 2);
    assertEquals(cluster.member(3).id(), 3);
  }

}
