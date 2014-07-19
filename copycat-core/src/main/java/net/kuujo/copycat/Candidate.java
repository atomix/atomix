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

import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.util.AsyncCallback;
import net.kuujo.copycat.util.Quorum;

/**
 * A candidate state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class Candidate extends BaseState {
  private static final Logger logger = Logger.getLogger(Candidate.class.getCanonicalName());
  private Quorum quorum;
  private final Timer electionTimer = new Timer();
  private final TimerTask electionTimerTask = new TimerTask() {
    @Override
    public void run() {
      // When the election times out, clear the previous majority vote
      // check and restart the election.
      logger.info("Election timed out");
      if (quorum != null) {
        quorum.cancel();
        quorum = null;
      }
      resetTimer();
      pollMembers();
      logger.info("Restarted election");
    }
  };

  Candidate(CopyCatContext context) {
    super(context);
  }

  @Override
  void init() {
    logger.info("Starting election");
    resetTimer();
    pollMembers();
  }

  /**
   * Resets the election timer.
   */
  private void resetTimer() {
    // When the election timer is reset, increment the current term.
    context.setCurrentTerm(context.getCurrentTerm() + 1);
    long timeout = context.config().getElectionTimeout() - (context.config().getElectionTimeout() / 4) + (Math.round(Math.random() * (context.config().getElectionTimeout() / 2)));
    electionTimer.schedule(electionTimerTask, timeout);
  }

  private void pollMembers() {
    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    if (quorum == null) {
      final Set<String> pollMembers = new HashSet<>(context.stateCluster.getMembers());
      quorum = new Quorum(context.stateCluster.getQuorumSize());
      quorum.setCallback(new AsyncCallback<Boolean>() {
        @Override
        public void complete(Boolean succeeded) {
          quorum = null;
          if (succeeded) {
            context.transition(CopyCatState.LEADER);
          } else {
            context.transition(CopyCatState.FOLLOWER);
          }
        }
        @Override
        public void fail(Throwable t) {
          quorum = null;
          context.transition(CopyCatState.FOLLOWER);
        }
      });

      // First, load the last log entry to get its term. We load the entry
      // by its index since the index is required by the protocol.
      long lastIndex = context.log.lastIndex();
      Entry lastEntry = context.log.getEntry(lastIndex);

      // Once we got the last log term, iterate through each current member
      // of the cluster and poll each member for a vote.
      final long lastTerm = lastEntry != null ? lastEntry.term() : 0;
      for (String member : pollMembers) {
        context.protocol.poll(member, new PollRequest(context.getCurrentTerm(), context.stateCluster.getLocalMember(), lastIndex, lastTerm), context.config().getElectionTimeout(), new AsyncCallback<PollResponse>() {
          @Override
          public void complete(PollResponse response) {
            if (quorum != null) {
              if (!response.voteGranted()) {
                quorum.fail();
              } else {
                quorum.succeed();
              }
            }
          }
          @Override
          public void fail(Throwable t) {
            quorum.fail();
          }
        });
      }
    }
  }

  @Override
  void ping(PingRequest request) {
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
      context.transition(CopyCatState.FOLLOWER);
    }
    request.respond(context.getCurrentTerm());
  }

  @Override
  void sync(final SyncRequest request) {
    boolean result = doSync(request);
    // If the request term is greater than the current term then this
    // indicates that another leader was already elected. Update the
    // current leader and term and transition back to a follower.
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
      context.transition(CopyCatState.FOLLOWER);
    }
    request.respond(context.getCurrentTerm(), result);
  }

  @Override
  void install(final InstallRequest request) {
    doInstall(request);

    // If the request term is greater than the current term then update
    // the current leader and term.
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
      context.transition(CopyCatState.FOLLOWER);
    }
    request.respond(context.getCurrentTerm());
  }

  @Override
  void poll(PollRequest request) {
    if (request.candidate().equals(context.cluster().getLocalMember())) {
      request.respond(context.getCurrentTerm(), true);
      context.setLastVotedFor(context.cluster().getLocalMember());
    } else {
      request.respond(context.getCurrentTerm(), false);
    }
  }

  @Override
  void submit(SubmitRequest request) {
    request.respond("Not a leader");
  }

  @Override
  void destroy() {
    electionTimer.cancel();
    if (quorum != null) {
      quorum.cancel();
      quorum = null;
    }
  }

}
