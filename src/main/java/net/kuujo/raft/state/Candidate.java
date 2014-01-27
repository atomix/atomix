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
package net.kuujo.raft.state;

import java.util.Set;

import net.kuujo.raft.log.CommandEntry;
import net.kuujo.raft.log.ConfigurationEntry;
import net.kuujo.raft.log.Entry;
import net.kuujo.raft.log.Entry.Type;
import net.kuujo.raft.protocol.PingRequest;
import net.kuujo.raft.protocol.PollRequest;
import net.kuujo.raft.protocol.PollResponse;
import net.kuujo.raft.protocol.SubmitRequest;
import net.kuujo.raft.protocol.SyncRequest;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * A candidate state.
 *
 * @author Jordan Halterman
 */
public class Candidate extends State {
  private Majority majority;
  private long electionTimer;

  @Override
  public void startUp(Handler<Void> doneHandler) {
    // When the candidate is created, increment the current term.
    context.setCurrentTerm(context.currentTerm() + 1);
    pollMembers();
    electionTimer = vertx.setTimer(context.electionTimeout(), new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        if (majority != null) {
          majority.cancel();
          majority = null;
          context.transition(StateType.FOLLOWER);
        }
        electionTimer = 0;
      }
    });
    doneHandler.handle((Void) null);
  }

  @Override
  public void configure(Set<String> members) {
    // Do nothing.
  }

  private void pollMembers() {
    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    if (majority == null) {
      majority = new Majority(context.members());
      majority.start(new Handler<String>() {
        @Override
        public void handle(final String address) {
          endpoint.poll(address, new PollRequest(context.currentTerm(), context.address(), log.lastIndex(), log.lastTerm()), new Handler<AsyncResult<PollResponse>>() {
            @Override
            public void handle(AsyncResult<PollResponse> result) {
              // If the election is null then that means it was already finished,
              // e.g. a majority of nodes responded.
              if (majority != null) {
                if (result.failed() || !result.result().voteGranted()) {
                  majority.fail(address);
                }
                else {
                  majority.succeed(address);
                }
              }
            }
          });
        }
      }, new Handler<Boolean>() {
        @Override
        public void handle(Boolean elected) {
          majority = null;
          if (elected) {
            context.transition(StateType.LEADER);
          }
          else {
            context.transition(StateType.FOLLOWER);
          }
        }
      });
    }
  }

  @Override
  public void handlePing(PingRequest request) {
    if (request.term() > context.currentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
      context.transition(StateType.FOLLOWER);
    }
    request.reply(context.currentTerm());
  }

  @Override
  public void handleSync(SyncRequest request) {
    if (request.term() < context.currentTerm()) {
      request.reply(context.currentTerm(), false);
    }
    else if (request.prevLogIndex() >= 0 && request.prevLogTerm() >= 0 && (!log.containsEntry(request.prevLogIndex())) || (log.entry(request.prevLogIndex()).term() != request.prevLogTerm())) {
      request.reply(context.currentTerm(), false);
    }
    else {
      // If a new log entry was provided, append the entry to the local log.
      if (request.entry() != null) {
        // If the given log entry conflicts with an entry in the log, remove the
        // conflicting entry and all entries after it.
        if (log.containsEntry(request.prevLogIndex() + 1) && log.entry(request.prevLogIndex() + 1).term() != request.entry().term()) {
          log.removeAfter(request.prevLogIndex());
        }
        // Append the entry to the log.
        log.appendEntry(request.entry());
      }

      // If there are entries to be committed, apply them to the state machine.
      if (request.commit() > context.commitIndex()) {
        context.setCommitIndex(Math.min(request.commit(), log.lastIndex()));
        log.floor(Math.min(context.commitIndex(), context.lastApplied()));
        for (long i = context.lastApplied(); i <= Math.min(context.commitIndex(), log.lastIndex()); ++i, context.setLastApplied(context.lastApplied()+1)) {
          Entry entry = log.entry(i);
          context.setLastApplied(i);
          log.floor(Math.min(context.commitIndex(), context.lastApplied()));
          if (entry.type().equals(Type.COMMAND)) {
            stateMachine.applyCommand(((CommandEntry) entry).command());
          }
          else if (entry.type().equals(Type.CONFIGURATION)) {
            context.configs().get(0).addAll(((ConfigurationEntry) entry).members());
            context.removeConfig();
          }
        }
      }
      request.reply(context.currentTerm(), true);
    }

    if (request.term() > context.currentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
      context.transition(StateType.FOLLOWER);
    }
  }

  @Override
  public void handlePoll(PollRequest request) {
    if (request.candidate().equals(context.address())) {
      request.reply(context.currentTerm(), true);
      context.setVotedFor(context.address());
    }
    else {
      request.reply(context.currentTerm(), false);
    }
  }

  @Override
  public void handleSubmit(SubmitRequest request) {
    request.error("Not a leader.");
  }

  @Override
  public void shutDown(Handler<Void> doneHandler) {
    if (electionTimer > 0) {
      vertx.cancelTimer(electionTimer);
      electionTimer = 0;
    }
    if (majority != null) {
      majority.cancel();
      majority = null;
    }
    doneHandler.handle((Void) null);
  }

}
