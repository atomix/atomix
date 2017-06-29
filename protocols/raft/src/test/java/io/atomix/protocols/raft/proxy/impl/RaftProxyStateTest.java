/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.proxy.impl;

import io.atomix.protocols.raft.ServiceName;
import io.atomix.protocols.raft.ServiceType;
import io.atomix.protocols.raft.session.SessionId;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * Client session state test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class RaftProxyStateTest {

  /**
   * Tests session state defaults.
   */
  public void testSessionStateDefaults() {
    String sessionName = UUID.randomUUID().toString();
    RaftProxyState state = new RaftProxyState(SessionId.from(1), ServiceName.from(sessionName), ServiceType.from("test"), 1000);
    assertEquals(state.getSessionId(), 0);
    assertEquals(state.getServiceName(), sessionName);
    assertEquals(state.getServiceType(), "test");
    assertEquals(state.getCommandRequest(), 0);
    assertEquals(state.getCommandResponse(), 0);
    assertEquals(state.getResponseIndex(), 0);
    assertEquals(state.getEventIndex(), 0);
  }

  /**
   * Tests updating client session state.
   */
  public void testSessionState() {
    RaftProxyState state = new RaftProxyState(SessionId.from(1), ServiceName.from(UUID.randomUUID().toString()), ServiceType.from("test"), 1000);
    assertEquals(state.getSessionId(), 1);
    assertEquals(state.getResponseIndex(), 1);
    assertEquals(state.getEventIndex(), 1);
    state.setCommandRequest(2);
    assertEquals(state.getCommandRequest(), 2);
    assertEquals(state.nextCommandRequest(), 3);
    assertEquals(state.getCommandRequest(), 3);
    state.setCommandResponse(3);
    assertEquals(state.getCommandResponse(), 3);
    state.setResponseIndex(4);
    assertEquals(state.getResponseIndex(), 4);
    state.setResponseIndex(3);
    assertEquals(state.getResponseIndex(), 4);
    state.setEventIndex(5);
    assertEquals(state.getEventIndex(), 5);
  }

}
