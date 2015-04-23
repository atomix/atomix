/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.protocol.raft;

import net.kuujo.copycat.protocol.raft.rpc.*;

import java.util.concurrent.CompletableFuture;

/**
 * Remote state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RemoteState extends RaftState {

  public RemoteState(RaftProtocol context) {
    super(context);
  }

  @Override
  public Type type() {
    return Type.REMOTE;
  }

  @Override
  protected CompletableFuture<AppendResponse> append(AppendRequest request) {
    return exceptionalFuture(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR.createException());
  }

  @Override
  protected CompletableFuture<SyncResponse> sync(SyncRequest request) {
    return exceptionalFuture(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR.createException());
  }

  @Override
  protected CompletableFuture<PollResponse> poll(PollRequest request) {
    return exceptionalFuture(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR.createException());
  }

  @Override
  protected CompletableFuture<VoteResponse> vote(VoteRequest request) {
    return exceptionalFuture(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR.createException());
  }

  @Override
  public CompletableFuture<ReadResponse> read(ReadRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(ReadResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getCluster().member(context.getLeader()).send(context.getTopic(), request);
    }
  }

  @Override
  public CompletableFuture<WriteResponse> write(WriteRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(WriteResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getCluster().member(context.getLeader()).send(context.getTopic(), request);
    }
  }

  @Override
  public CompletableFuture<DeleteResponse> delete(DeleteRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(DeleteResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getCluster().member(context.getLeader()).send(context.getTopic(), request);
    }
  }

}
