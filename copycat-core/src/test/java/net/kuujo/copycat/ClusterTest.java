/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.MemberConfig;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Cluster test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterTest {

  @Test
  public void testMemberConfigure() {
    MemberConfig config = new MemberConfig();
    config.setId("foo");
    assertEquals("foo", config.getId());
    config.withId("bar");
    assertEquals("bar", config.getId());
    Member member = new Member(config);
    assertEquals("bar", member.id());
  }

  @Test
  public void testClusterConfigure() {
    ClusterConfig<Member> config = new ClusterConfig<>();
    Member localMember = new Member(new MemberConfig("foo"));
    config.setLocalMember(localMember);
    assertEquals(localMember, config.getLocalMember());
    Set<Member> remoteMembers = new HashSet<>();
    Member remoteMember1 = new Member(new MemberConfig("bar"));
    Member remoteMember2 = new Member(new MemberConfig("baz"));
    remoteMembers.add(remoteMember1);
    remoteMembers.add(remoteMember2);
    config.setRemoteMembers(remoteMembers);
    assertTrue(config.getRemoteMembers().contains(remoteMember1));
    assertTrue(config.getRemoteMembers().contains(remoteMember2));
  }

}
