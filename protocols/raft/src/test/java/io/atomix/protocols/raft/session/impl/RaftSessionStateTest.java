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
package io.atomix.protocols.raft.session.impl;

import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.TestPrimitiveType;
import io.atomix.protocols.raft.session.impl.RaftSessionState;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Client session state test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftSessionStateTest {

  /**
   * Tests session state defaults.
   */
  @Test
  public void testSessionStateDefaults() {
    String sessionName = UUID.randomUUID().toString();
    RaftSessionState state = new RaftSessionState("test", SessionId.from(1), sessionName, TestPrimitiveType.instance(), 1000);
    assertEquals(state.getSessionId(), SessionId.from(1));
    assertEquals(state.getPrimitiveName(), sessionName);
    assertEquals(state.getPrimitiveType().name(), "test");
    assertEquals(state.getCommandRequest(), 0);
    assertEquals(state.getCommandResponse(), 0);
    assertEquals(state.getResponseIndex(), 1);
    assertEquals(state.getEventIndex(), 1);
  }

  /**
   * Tests updating client session state.
   */
  @Test
  public void testSessionState() {
    RaftSessionState state = new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000);
    assertEquals(state.getSessionId(), SessionId.from(1));
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
