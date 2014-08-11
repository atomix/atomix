/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.AsyncCallback;

/**
 * Protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ProtocolClient extends ProtocolHandler {

  /**
   * Sends a sync request.
   *
   * @param request The sync request.
   * @param callback A callback to be called once the response is received.
   */
  void appendEntries(AppendEntriesRequest request, AsyncCallback<AppendEntriesResponse> callback);

  /**
   * Sends an install request.
   *
   * @param request The install request.
   * @param callback A callback to be called once the response is received.
   */
  void installSnapshot(InstallSnapshotRequest request, AsyncCallback<InstallSnapshotResponse> callback);

  /**
   * Sends a poll request.
   *
   * @param request The poll request.
   * @param callback A callback to be called once the response is received.
   */
  void requestVote(RequestVoteRequest request, AsyncCallback<RequestVoteResponse> callback);

  /**
   * Sends a submit command request.
   *
   * @param request The submit request.
   * @param callback A callback to be called once the response is received.
   */
  void submitCommand(SubmitCommandRequest request, AsyncCallback<SubmitCommandResponse> callback);

  /**
   * Connects the client.
   */
  void connect();

  /**
   * Connects the client.
   *
   * @param callback A callback to be called once connected.
   */
  void connect(AsyncCallback<Void> callback);

  /**
   * Closes the client.
   */
  void close();

  /**
   * Closes the client.
   *
   * @param callback A callback to be called once closed.
   */
  void close(AsyncCallback<Void> callback);

}
