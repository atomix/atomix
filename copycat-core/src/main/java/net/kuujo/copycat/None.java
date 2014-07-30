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
package net.kuujo.copycat;

import net.kuujo.copycat.protocol.InstallSnapshotRequest;
import net.kuujo.copycat.protocol.InstallSnapshotResponse;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * Non-existent state.<p>
 *
 * The <code>None</code> state is used to represent a non-existent
 * state in a CopyCat context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class None extends BaseState {

  @Override
  public void init(CopyCatContext context) {
    // Don't call super.init() here so that server handlers won't be registered.
    context.setCurrentLeader(null);
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncCallback<AppendEntriesResponse> responseCallback) {
    responseCallback.complete(new AppendEntriesResponse("Replica is not alive"));
  }

  @Override
  public void installSnapshot(InstallSnapshotRequest request, AsyncCallback<InstallSnapshotResponse> responseCallback) {
    responseCallback.complete(new InstallSnapshotResponse("Replica is not alive"));
  }

  @Override
  public void requestVote(RequestVoteRequest request, AsyncCallback<RequestVoteResponse> responseCallback) {
    responseCallback.complete(new RequestVoteResponse("Replica is not alive"));
  }

  @Override
  public void submitCommand(SubmitCommandRequest request, AsyncCallback<SubmitCommandResponse> responseCallback) {
    responseCallback.complete(new SubmitCommandResponse("Replica is not alive"));
  }

  @Override
  public void destroy() {
    // Do nothing.
  }

}
