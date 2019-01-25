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

import java.util.UUID;

import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.TestPrimitiveType;
import org.junit.Test;

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
    assertEquals(sessionName, state.getPrimitiveName());
    assertEquals("test", state.getPrimitiveType().name());
    assertEquals(0, state.getCommandRequest());
    assertEquals(0, state.getCommandResponse());
    assertEquals(1, state.getResponseIndex());
    assertEquals(1, state.getEventIndex());
  }

  /**
   * Tests updating client session state.
   */
  @Test
  public void testSessionState() {
    RaftSessionState state = new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000);
    assertEquals(state.getSessionId(), SessionId.from(1));
    assertEquals(1, state.getResponseIndex());
    assertEquals(1, state.getEventIndex());
    state.setCommandRequest(2);
    assertEquals(2, state.getCommandRequest());
    assertEquals(3, state.nextCommandRequest());
    assertEquals(3, state.getCommandRequest());
    state.setCommandResponse(3);
    assertEquals(3, state.getCommandResponse());
    state.setResponseIndex(4);
    assertEquals(4, state.getResponseIndex());
    state.setResponseIndex(3);
    assertEquals(4, state.getResponseIndex());
    state.setEventIndex(5);
    assertEquals(5, state.getEventIndex());
  }

}
