/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.primitive.partition.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.MemberGroupId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.partition.impl.PrimaryElectorOperations.Enter;
import io.atomix.primitive.partition.impl.PrimaryElectorOperations.GetTerm;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrimaryElectorServiceTest {
  static long sessionNum = 0;

  @Test
  public void testEnterSinglePartition() {
    PartitionId partition = new PartitionId("test", 1);
    PrimaryElectorService elector = newService();
    PrimaryTerm term;

    // 1st member to enter should be primary.
    GroupMember m1 = createGroupMember("node1", "group1");
    Session<?> s1 = createSession(m1);
    term = elector.enter(createEnterOp(partition, m1, s1));
    assertEquals(1L, term.term());
    assertEquals(m1, term.primary());
    assertEquals(1, term.candidates().size());

    // 2nd member to enter should be added to candidates.
    GroupMember m2 = createGroupMember("node2", "group1");
    Session<?> s2 = createSession(m2);
    term = elector.enter(createEnterOp(partition, m2, s2));
    assertEquals(1L, term.term());
    assertEquals(m1, term.primary());
    assertEquals(2, term.candidates().size());
    assertEquals(m2, term.candidates().get(1));
  }

  @Test
  public void testEnterSeveralPartitions() {
    PrimaryElectorService elector = newService();
    PrimaryTerm term = null;
    int numParts = 10;
    int numMembers = 20;

    List<List<GroupMember>> allMembers = new ArrayList<>();
    List<PrimaryTerm> terms = new ArrayList<>();
    for (int p = 0; p < numParts; p++) {
      PartitionId partId = new PartitionId("test", p);
      allMembers.add(new ArrayList<>());

      // Add all members in same group.
      for (int i = 0; i < numMembers; i++) {
        GroupMember m = createGroupMember("node" + i, "group1");
        allMembers.get(p).add(m);
        Session<?> s = createSession(m);
        term = elector.enter(createEnterOp(partId, m, s));
      }

      if (term != null) {
        terms.add(term);
      }
    }

    // Check primary and candidates in each partition.
    for (int p = 0; p < numParts; p++) {
      assertEquals(1L, terms.get(p).term());
      assertEquals(allMembers.get(p).get(0), terms.get(p).primary());
      assertEquals(numMembers, terms.get(p).candidates().size());
      for (int i = 0; i < numMembers; i++) {
        assertEquals(allMembers.get(p).get(i), terms.get(p).candidates().get(i));
      }
    }
  }

  @Test
  public void testEnterSinglePartitionWithGroups() {
    PrimaryElectorService elector = newService();
    PartitionId partId = new PartitionId("test", 1);
    PrimaryTerm term = null;
    int numMembers = 9;

    // Add 9 members in 3 different groups.
    List<GroupMember> members = new ArrayList<>();
    for (int i = 0; i < numMembers; i++) {
      GroupMember m = createGroupMember("node" + i, "group" + (i / 3));
      members.add(m);
      Session<?> s = createSession(m);
      term = elector.enter(createEnterOp(partId, m, s));
    }

    // Check primary and candidates.
    assertEquals(1L, term.term());
    assertEquals(members.get(0), term.primary());
    assertEquals(numMembers, term.candidates().size());

    // Check backups are selected in different groups.
    List<GroupMember> backups2 = term.backups(2);
    assertEquals(members.get(3), backups2.get(0));
    assertEquals(members.get(6), backups2.get(1));

    List<GroupMember> backups3 = term.backups(3);
    assertEquals(members.get(3), backups3.get(0));
    assertEquals(members.get(6), backups3.get(1));
    assertEquals(members.get(1), backups3.get(2));
  }

  @Test
  public void testEnterAndExpireSessions() {
    PrimaryElectorService elector = newService();
    PartitionId partId = new PartitionId("test", 1);
    PrimaryTerm term = null;
    int numMembers = 9;

    // Add 9 members in 3 different groups.
    List<Session<?>> sessions = new ArrayList<>();
    List<GroupMember> members = new ArrayList<>();
    for (int i = 0; i < numMembers; i++) {
      GroupMember m = createGroupMember("node" + i, "group" + (i / 3));
      members.add(m);
      Session<?> s = createSession(m);
      sessions.add(s);
      term = elector.enter(createEnterOp(partId, m, s));
    }

    // Check current primary.
    assertEquals(1L, term.term());
    assertEquals(members.get(0), term.primary());
    assertEquals(numMembers, term.candidates().size());
    List<GroupMember> backups1 = term.backups(2);
    assertEquals(members.get(3), backups1.get(0));
    assertEquals(members.get(6), backups1.get(1));

    // Expire session of primary and check new term.
    // New primary should be the first of the old backups.
    elector.onExpire(sessions.get(0));
    term = elector.getTerm(createGetTermOp(partId, members.get(3), sessions.get(3)));
    assertEquals(2L, term.term());
    assertEquals(members.get(3), term.primary());
    assertEquals(numMembers - 1, term.candidates().size());
    List<GroupMember> backups2 = term.backups(2);
    assertEquals(members.get(6), backups2.get(0));
    assertEquals(members.get(1), backups2.get(1));

    // Expire session of backup and check term updated.
    elector.onExpire(sessions.get(6));
    term = elector.getTerm(createGetTermOp(partId, members.get(5), sessions.get(5)));
    assertEquals(2L, term.term());
    assertEquals(members.get(3), term.primary());
    assertEquals(numMembers - 2, term.candidates().size());
    List<GroupMember> backups3 = term.backups(2);
    assertEquals(members.get(1), backups3.get(0));
    assertEquals(members.get(4), backups3.get(1));
  }

  @Test
  public void testSortCandidatesByGroup() {
    PrimaryElectorService elector = newService();
    PrimaryTerm term = null;

    term = enter("node1", "group1", elector);
    assertEquals("node1", term.primary().memberId().id());

    term = enter("node2", "group1", elector);
    assertEquals("node1", term.primary().memberId().id());
    assertEquals("node2", term.candidates().get(1).memberId().id());
    assertEquals("node2", term.backups(2).get(0).memberId().id());

    term = enter("node3", "group1", elector);
    assertEquals("node1", term.primary().memberId().id());
    assertEquals("node2", term.candidates().get(1).memberId().id());
    assertEquals("node2", term.backups(2).get(0).memberId().id());
    assertEquals("node3", term.candidates().get(2).memberId().id());
    assertEquals("node3", term.backups(2).get(1).memberId().id());

    term = enter("node4", "group2", elector);
    assertEquals("node1", term.primary().memberId().id());
    assertEquals("node4", term.candidates().get(1).memberId().id());
    assertEquals("node4", term.backups(2).get(0).memberId().id());
    assertEquals("node2", term.candidates().get(2).memberId().id());
    assertEquals("node2", term.backups(2).get(1).memberId().id());

    term = enter("node5", "group3", elector);
    assertEquals("node1", term.primary().memberId().id());
    assertEquals("node4", term.candidates().get(1).memberId().id());
    assertEquals("node4", term.backups(2).get(0).memberId().id());
    assertEquals("node5", term.candidates().get(2).memberId().id());
    assertEquals("node5", term.backups(2).get(1).memberId().id());

    term = enter("node6", "group3", elector);
    assertEquals("node1", term.primary().memberId().id());
    assertEquals("node4", term.candidates().get(1).memberId().id());
    assertEquals("node4", term.backups(2).get(0).memberId().id());
    assertEquals("node5", term.candidates().get(2).memberId().id());
    assertEquals("node5", term.backups(2).get(1).memberId().id());

    assertEquals("node1", term.candidates().get(0).memberId().id());
    assertEquals("node4", term.candidates().get(1).memberId().id());
    assertEquals("node5", term.candidates().get(2).memberId().id());
    assertEquals("node2", term.candidates().get(3).memberId().id());
    assertEquals("node6", term.candidates().get(4).memberId().id());
    assertEquals("node3", term.candidates().get(5).memberId().id());
  }

  @Test
  public void testSortCandidatesWithoutGroup() {
    PrimaryElectorService elector = newService();
    PrimaryTerm term = null;

    term = enter("node1", "node1", elector);
    term = enter("node2", "node2", elector);
    term = enter("node3", "node3", elector);
    term = enter("node4", "node4", elector);
    term = enter("node5", "node5", elector);
    term = enter("node6", "node6", elector);

    assertEquals("node1", term.candidates().get(0).memberId().id());
    assertEquals("node2", term.candidates().get(1).memberId().id());
    assertEquals("node3", term.candidates().get(2).memberId().id());
    assertEquals("node4", term.candidates().get(3).memberId().id());
    assertEquals("node5", term.candidates().get(4).memberId().id());
    assertEquals("node6", term.candidates().get(5).memberId().id());
  }

  private PrimaryTerm enter(String nodeId, String groupId, PrimaryElectorService elector) {
    PartitionId partId = new PartitionId("test", 1);
    GroupMember member = createGroupMember(nodeId, groupId);
    Session session = createSession(member);
    return elector.enter(createEnterOp(partId, member, session));
  }

  Commit<Enter> createEnterOp(PartitionId partition, GroupMember member, Session<?> session) {
    Enter enter = new Enter(partition, member);
    return new DefaultCommit<>(0, null, enter, session, System.currentTimeMillis());
  }

  Commit<GetTerm> createGetTermOp(PartitionId partition, GroupMember member, Session<?> session) {
    GetTerm getTerm = new GetTerm(partition);
    return new DefaultCommit<>(0, null, getTerm, session, System.currentTimeMillis());
  }

  GroupMember createGroupMember(String id, String groupId) {
    return new GroupMember(MemberId.from(id), groupId != null ? MemberGroupId.from(groupId) : null);
  }

  PrimaryElectorService newService() {
    PrimaryElectorService elector = new PrimaryElectorService();
    elector.init(new ServiceContext() {
      @Override
      public PrimitiveId serviceId() {
        return PrimitiveId.from(1L);
      }

      @Override
      public String serviceName() {
        return "test-primary-elector";
      }

      @SuppressWarnings("rawtypes")
      @Override
      public PrimitiveType serviceType() {
        return PrimaryElectorType.instance();
      }

      @Override
      public MemberId localMemberId() {
        return null;
      }

      @Override
      public <C extends ServiceConfig> C serviceConfig() {
        return null;
      }

      @Override
      public long currentIndex() {
        return 0;
      }

      @Override
      public Session<?> currentSession() {
        return null;
      }

      @Override
      public OperationType currentOperation() {
        return null;
      }

      @Override
      public LogicalClock logicalClock() {
        return null;
      }

      @Override
      public WallClock wallClock() {
        return null;
      }
    });
    elector.tick(WallClockTimestamp.from(System.currentTimeMillis()));
    return elector;
  }

  @SuppressWarnings("rawtypes")
  Session<?> createSession(final GroupMember member) {
    return new Session() {
      long sessionId = sessionNum++;

      @Override
      public SessionId sessionId() {
        return SessionId.from(sessionId);
      }

      @Override
      public String primitiveName() {
        return null; // not used in test
      }

      @Override
      public PrimitiveType primitiveType() {
        return null; // not used in test
      }

      @Override
      public MemberId memberId() {
        return member.memberId();
      }

      @Override
      public State getState() {
        return State.OPEN;
      }

      @Override
      public void publish(EventType eventType, Object event) {
        // not used in test
      }

      @Override
      public void publish(PrimitiveEvent event) {
        // not used in test
      }

      @Override
      public void accept(Consumer event) {
        // not used in test
      }

      @Override
      public String toString() {
        return "Session " + sessionId;
      }
    };
  }
}
