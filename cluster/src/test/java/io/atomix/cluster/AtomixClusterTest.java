/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Atomix cluster test.
 */
public class AtomixClusterTest {
  @Test
  public void testMembers() throws Exception {
    Collection<Member> members = Arrays.asList(
        Member.builder("foo")
            .withType(Member.Type.EPHEMERAL)
            .withAddress("localhost:5000")
            .build(),
        Member.builder("bar")
            .withType(Member.Type.EPHEMERAL)
            .withAddress("localhost:5001")
            .build(),
        Member.builder("baz")
            .withType(Member.Type.EPHEMERAL)
            .withAddress("localhost:5002")
            .build());

    AtomixCluster cluster1 = AtomixCluster.builder()
        .withLocalMember("foo")
        .withMembers(members)
        .build();
    cluster1.start().join();

    assertEquals("foo", cluster1.membershipService().getLocalMember().id().id());

    AtomixCluster cluster2 = AtomixCluster.builder()
        .withLocalMember("bar")
        .withMembers(members)
        .build();
    cluster2.start().join();

    assertEquals("bar", cluster2.membershipService().getLocalMember().id().id());

    AtomixCluster cluster3 = AtomixCluster.builder()
        .withLocalMember("baz")
        .withMembers(members)
        .build();
    cluster3.start().join();

    assertEquals("baz", cluster3.membershipService().getLocalMember().id().id());
  }
}
