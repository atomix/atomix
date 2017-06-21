/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.cluster.MemberId;

import java.util.Map;

/**
 * Test server protocol.
 */
public class TestRaftServerProtocol extends TestRaftProtocol implements RaftServerProtocol {
  private final TestRaftServerListener listener = new TestRaftServerListener();
  private final TestRaftServerDispatcher dispatcher = new TestRaftServerDispatcher(this);

  public TestRaftServerProtocol(Map<MemberId, TestRaftServerProtocol> servers, Map<MemberId, TestRaftClientProtocol> clients) {
    super(servers, clients);
  }

  @Override
  public TestRaftServerListener listener() {
    return listener;
  }

  @Override
  public TestRaftServerDispatcher dispatcher() {
    return dispatcher;
  }
}
