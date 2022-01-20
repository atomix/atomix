/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.election.impl;

import io.atomix.core.election.LeaderElectionType;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.LeaderElectorTest;
import io.atomix.core.election.Leadership;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.time.WallClock;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Leader elector service test.
 */
public class DefaultLeaderElectorServiceTest {

  String node1 = "4";
  String node2 = "5";
  String node3 = "6";

  @Test
  public void testDuplicateCandidateNode() {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceType()).thenReturn(LeaderElectionType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));
    when(context.wallClock()).thenReturn(new WallClock());

    Session session = mock(Session.class);
    when(session.sessionId()).thenReturn(SessionId.from(1));
    when(context.currentSession()).thenReturn(session);

    DefaultLeaderElectorService service = new DefaultLeaderElectorService();
    service.init(context);
    service.register(session);

    byte[] id1 = "a".getBytes();
    service.run("foo", id1);

    byte[] id2 = "a".getBytes();
    service.run("foo", id2);

    // To test that duplicate candidate node entry is rejected
    assertEquals(1, service.getLeaderships().size());
  }

  @Test
  public void testDemote() {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceType()).thenReturn(LeaderElectionType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));
    when(context.wallClock()).thenReturn(new WallClock());

    Session session = mock(Session.class);
    when(session.sessionId()).thenReturn(SessionId.from(1));
    when(context.currentSession()).thenReturn(session);

    DefaultLeaderElectorService service = new DefaultLeaderElectorService();
    service.init(context);
    service.register(session);

    byte[] id1 = node1.getBytes();
    byte[] id2 = node2.getBytes();
    byte[] id3 = node3.getBytes();

    // leadership is null
    assertFalse(service.demote("foo", id3));

    service.run("foo", id1);

    when(session.sessionId()).thenReturn(SessionId.from(2));
    when(context.currentSession()).thenReturn(session);
    service.register(session);
    service.run("foo", id2);

    // not part of the leadership
    assertFalse(service.demote("foo", id3));

    // leader cannot be demoted
    assertEquals(service.getLeadership("foo").leader().id(), id1);
    assertFalse(service.demote("foo", id1));

    when(session.sessionId()).thenReturn(SessionId.from(3));
    when(context.currentSession()).thenReturn(session);
    service.register(session);
    service.run("foo", id3);

    Leadership<byte[]> leadership = service.getLeadership("foo");
    Assert.assertEquals(id3, leadership.candidates().get(2));

    // demote node2
    assertTrue(service.demote("foo", id2));
    leadership = service.getLeadership("foo");
    Assert.assertEquals(id3, leadership.candidates().get(1));
    Assert.assertEquals(id2, leadership.candidates().get(2));

    // demote leader by displacing it first
    assertTrue(service.promote("foo", id2));
    leadership = service.getLeadership("foo");
    Assert.assertEquals(id1, leadership.leader().id());
    Assert.assertEquals(id2, leadership.candidates().get(0));
    Assert.assertEquals(id1, leadership.candidates().get(1));
    Assert.assertEquals(id3, leadership.candidates().get(2));

    assertTrue(service.anoint("foo", id2));
    leadership = service.getLeadership("foo");
    Assert.assertEquals(id2, leadership.leader().id());
    Assert.assertEquals(id2, leadership.candidates().get(0));
    Assert.assertEquals(id1, leadership.candidates().get(1));
    Assert.assertEquals(id3, leadership.candidates().get(2));

    assertTrue(service.demote("foo", id1));
    leadership = service.getLeadership("foo");
    Assert.assertEquals(id2, leadership.leader().id());
    Assert.assertEquals(id2, leadership.candidates().get(0));
    Assert.assertEquals(id3, leadership.candidates().get(1));
    Assert.assertEquals(id1, leadership.candidates().get(2));
  }

}
