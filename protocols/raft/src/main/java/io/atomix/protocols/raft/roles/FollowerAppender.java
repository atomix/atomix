/*
 * Copyright 2016-present Open Networking Laboratory
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
 * limitations under the License
 */
package io.atomix.protocols.raft.roles;

import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.RaftMemberContext;
import io.atomix.protocols.raft.impl.RaftServerContext;
import io.atomix.protocols.raft.storage.snapshot.Snapshot;

/**
 * Follower appender.
 */
final class FollowerAppender extends AbstractAppender {

  public FollowerAppender(RaftServerContext context) {
    super(context);
  }

  /**
   * Sends append requests to assigned passive members.
   */
  public void appendEntries() {
    if (open) {
      for (RaftMemberContext member : server.getClusterState().getAssignedPassiveMemberStates()) {
        member.getThreadContext().execute(() -> appendEntries(member));
      }
    }
  }

  @Override
  protected boolean hasMoreEntries(RaftMemberContext member) {
    return member.getMember().getType() == RaftMember.Type.PASSIVE
        && member.getNextIndex() <= server.getCommitIndex()
        && member.getLogReader().hasNext();
  }

  @Override
  protected void appendEntries(RaftMemberContext member) {
    // Prevent recursive, asynchronous appends from being executed if the appender has been closed.
    if (!open) {
      return;
    }

    // If the member's current snapshot index is less than the latest snapshot index and the latest snapshot index
    // is less than the nextIndex, send a snapshot request.
    Snapshot snapshot = server.getSnapshotStore().getSnapshotByIndex(member.getNextIndex());
    if (snapshot != null) {
      if (member.canInstall()) {
        sendInstallRequest(member, buildInstallRequest(member));
      }
    }
    // If no AppendRequest is already being sent, send an AppendRequest.
    else if (member.canAppend() && hasMoreEntries(member)) {
      sendAppendRequest(member, buildAppendRequest(member, Math.min(server.getCommitIndex(), server.getLogWriter().getLastIndex())));
    }
  }

}
