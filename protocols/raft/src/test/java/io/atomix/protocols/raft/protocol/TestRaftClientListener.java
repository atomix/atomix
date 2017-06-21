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

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Test Raft client listener.
 */
public class TestRaftClientListener implements RaftClientProtocolListener {
  private final Map<Long, Consumer<PublishRequest>> publishListeners = Maps.newConcurrentMap();

  void publish(PublishRequest request) {
    Consumer<PublishRequest> listener = publishListeners.get(request.session());
    if (listener != null) {
      listener.accept(request);
    }
  }

  @Override
  public void registerPublishListener(long sessionId, Consumer<PublishRequest> listener, Executor executor) {
    publishListeners.put(sessionId, request -> executor.execute(() -> listener.accept(request)));
  }

  @Override
  public void unregisterPublishListener(long sessionId) {
    publishListeners.remove(sessionId);
  }
}
