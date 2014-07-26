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
import net.kuujo.copycat.protocol.InstallResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.util.AsyncCallback;
import net.kuujo.copycat.util.Quorum;

/**
 * Candidate replica state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Candidate extends BaseState {
  private static final Logger logger = Logger.getLogger(Candidate.class.getCanonicalName());
  private Quorum quorum;
  private final Timer electionTimer = new Timer();
  private TimerTask electionTimerTask;

  @Override
  public void init(CopyCatContext context) {
    super.init(context);
    logger.info(String.format("%s starting election", context.cluster.config().getLocalMember()));
    resetTimer();
  }

  /**
   * Resets the election timer.
   */
  private synchronized void resetTimer() {
    // Cancel the current timer task and purge the election timer of cancelled tasks.
    if (electionTimerTask != null) {
      electionTimerTask.cancel();
      electionTimer.purge();
    }

    electionTimerTask = new TimerTask() {
      @Override
      public void run() {
        // When the election times out, clear the previous majority vote
        // check and restart the election.
        logger.info(String.format("%s election timed out", context.cluster.config().getLocalMember()));
        if (quorum != null) {
          quorum.cancel();
          quorum = null;
        }
        resetTimer();
        logger.info(String.format("%s restarted election", context.cluster.config().getLocalMember()));
      }
    };

    // When the election timer is reset, increment the current term.
    context.setCurrentTerm(context.getCurrentTerm() + 1);
    long timeout = context.config().getElectionTimeout() - (context.config().getElectionTimeout() / 4) + (Math.round(Math.random() * (context.config().getElectionTimeout() / 2)));
    electionTimer.schedule(electionTimerTask, timeout);
    pollMembers();
  }

  /**
   * Polls all the members of the cluster for votes.
   */
  private void pollMembers() {
    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    if (quorum == null) {
      final Set<String> pollMembers = new HashSet<>(context.cluster.config().getMembers());
      quorum = new Quorum(context.cluster.config().getQuorumSize());
      quorum.setCallback(new AsyncCallback<Boolean>() {
        @Override
        public void complete(Boolean succeeded) {
          quorum = null;
          if (succeeded) {
            context.transition(Leader.class);
          } else {
            context.transition(Follower.class);
          }
        }
        @Override
        public void fail(Throwable t) {
          quorum = null;
          context.transition(Follower.class);
        }
      });

      // First, load the last log entry to get its term. We load the entry
      // by its index since the index is required by the protocol.
      long lastIndex = context.log.lastIndex();
      Entry lastEntry = context.log.getEntry(lastIndex);

      // Once we got the last log term, iterate through each current member
      // of the cluster and poll each member for a vote.
      final long lastTerm = lastEntry != null ? lastEntry.term() : 0;
      for (final String member : pollMembers) {
        context.cluster.member(member).protocol().client().poll(new PollRequest(context.getCurrentTerm(), context.cluster.config().getLocalMember(), lastIndex, lastTerm), new AsyncCallback<PollResponse>() {
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
  public void sync(SyncRequest request, AsyncCallback<SyncResponse> responseCallback) {
    super.sync(request, responseCallback);
  }

  @Override
  public void install(InstallRequest request, AsyncCallback<InstallResponse> responseCallback) {
    super.install(request, responseCallback);
  }

  @Override
  public void poll(PollRequest request, AsyncCallback<PollResponse> responseCallback) {
    super.poll(request, responseCallback);
  }

  @Override
  public void destroy() {
    electionTimer.cancel();
    if (quorum != null) {
      quorum.cancel();
      quorum = null;
    }
  }

}
