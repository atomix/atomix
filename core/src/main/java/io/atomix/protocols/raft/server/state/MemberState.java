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
package io.atomix.protocols.raft.server.state;

import io.atomix.protocols.raft.server.storage.Log;
import io.atomix.protocols.raft.server.storage.LogReader;
import io.atomix.protocols.raft.server.storage.Reader;
import io.atomix.util.concurrent.ThreadContext;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster member state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class MemberState {
  private static final int MAX_APPENDS = 2;
  private final RaftMemberState member;
  final ThreadContext context;
  private long term;
  private long configIndex;
  private long nextSnapshotIndex;
  private int nextSnapshotOffset;
  private long matchIndex;
  private long nextIndex;
  private long heartbeatTime;
  private long heartbeatStartTime;
  private int appending;
  private boolean appendSucceeded;
  private long appendTime;
  private boolean configuring;
  private boolean installing;
  private volatile int failures;
  private volatile LogReader reader;
  private final TimeBuffer timeBuffer = new TimeBuffer(8);

  MemberState(RaftMemberState member, RaftClusterState cluster, ThreadContext context) {
    this.member = checkNotNull(member, "member cannot be null").setCluster(cluster);
    this.context = checkNotNull(context, "context cannot be null");
  }

  /**
   * Resets the member state.
   */
  void resetState(Log log) {
    nextSnapshotIndex = 0;
    nextSnapshotOffset = 0;
    matchIndex = 0;
    nextIndex = log.writer().lastIndex() + 1;
    heartbeatTime = 0;
    heartbeatStartTime = 0;
    appending = 0;
    timeBuffer.reset();
    configuring = false;
    installing = false;
    appendSucceeded = false;
    failures = 0;

    switch (member.type()) {
      case PASSIVE:
        reader = log.createReader(log.writer().lastIndex() + 1, Reader.Mode.COMMITS);
        break;
      case ACTIVE:
        reader = log.createReader(log.writer().lastIndex() + 1, Reader.Mode.ALL);
        break;
    }
  }

  /**
   * Returns the member.
   *
   * @return The member.
   */
  public RaftMemberState getMember() {
    return member;
  }

  /**
   * Returns the member log reader.
   *
   * @return The member log reader.
   */
  LogReader getLogReader() {
    return reader;
  }

  /**
   * Returns the member term.
   *
   * @return The member term.
   */
  long getConfigTerm() {
    return term;
  }

  /**
   * Sets the member term.
   *
   * @param term The member term.
   * @return The member state.
   */
  MemberState setConfigTerm(long term) {
    this.term = term;
    return this;
  }

  /**
   * Returns the member configuration index.
   *
   * @return The member configuration index.
   */
  long getConfigIndex() {
    return configIndex;
  }

  /**
   * Sets the member configuration index.
   *
   * @param configIndex The member configuration index.
   */
  void setConfigIndex(long configIndex) {
    this.configIndex = configIndex;
  }

  /**
   * Returns the member's next snapshot index.
   *
   * @return The member's next snapshot index.
   */
  long getNextSnapshotIndex() {
    return nextSnapshotIndex;
  }

  /**
   * Sets the member's next snapshot index.
   *
   * @param nextSnapshotIndex The member's next snapshot index.
   * @return The member state.
   */
  MemberState setNextSnapshotIndex(long nextSnapshotIndex) {
    this.nextSnapshotIndex = nextSnapshotIndex;
    return this;
  }

  /**
   * Returns the member's snapshot offset.
   *
   * @return The member's snapshot offset.
   */
  int getNextSnapshotOffset() {
    return nextSnapshotOffset;
  }

  /**
   * Sets the member's snapshot offset.
   *
   * @param nextSnapshotOffset The member's snapshot offset.
   * @return The member state.
   */
  MemberState setNextSnapshotOffset(int nextSnapshotOffset) {
    this.nextSnapshotOffset = nextSnapshotOffset;
    return this;
  }

  /**
   * Returns the member's match index.
   *
   * @return The member's match index.
   */
  long getMatchIndex() {
    return matchIndex;
  }

  /**
   * Sets the member's match index.
   *
   * @param matchIndex The member's match index.
   */
  void setMatchIndex(long matchIndex) {
    checkArgument(matchIndex >= 0, "matchIndex must be positive");
    this.matchIndex = matchIndex;
  }

  /**
   * Returns the member's next index.
   *
   * @return The member's next index.
   */
  long getNextIndex() {
    return nextIndex;
  }

  /**
   * Sets the member's next index.
   *
   * @param nextIndex The member's next index.
   * @return The member state.
   */
  MemberState setNextIndex(long nextIndex) {
    checkArgument(nextIndex > 0, "nextIndex must be positive");
    this.nextIndex = nextIndex;
    return this;
  }

  /**
   * Returns a boolean indicating whether an append request can be sent to the member.
   *
   * @return Indicates whether an append request can be sent to the member.
   */
  boolean canAppend() {
    return appending == 0 || (appendSucceeded && appending < MAX_APPENDS && System.nanoTime() - (timeBuffer.average() / MAX_APPENDS) >= appendTime);
  }

  /**
   * Flags the last append to the member as successful.
   */
  void appendSucceeded() {
    appendSucceeded(true);
  }

  /**
   * Flags the last append to the member is failed.
   */
  void appendFailed() {
    appendSucceeded(false);
  }

  /**
   * Sets whether the last append to the member succeeded.
   *
   * @param succeeded Whether the last append to the member succeeded.
   */
  private void appendSucceeded(boolean succeeded) {
    this.appendSucceeded = succeeded;
  }

  /**
   * Starts an append request to the member.
   */
  void startAppend() {
    appending++;
    appendTime = System.nanoTime();
  }

  /**
   * Completes an append request to the member.
   */
  void completeAppend() {
    appending--;
  }

  /**
   * Completes an append request to the member.
   *
   * @param time The time in milliseconds for the append.
   */
  void completeAppend(long time) {
    timeBuffer.record(time);
  }

  /**
   * Returns a boolean indicating whether a configure request can be sent to the member.
   *
   * @return Indicates whether a configure request can be sent to the member.
   */
  boolean canConfigure() {
    return !configuring;
  }

  /**
   * Starts a configure request to the member.
   */
  void startConfigure() {
    configuring = true;
  }

  /**
   * Completes a configure request to the member.
   */
  void completeConfigure() {
    configuring = false;
  }

  /**
   * Returns a boolean indicating whether an install request can be sent to the member.
   *
   * @return Indicates whether an install request can be sent to the member.
   */
  boolean canInstall() {
    return !installing;
  }

  /**
   * Starts an install request to the member.
   */
  void startInstall() {
    installing = true;
  }

  /**
   * Completes an install request to the member.
   */
  void completeInstall() {
    installing = false;
  }

  /**
   * Returns the member heartbeat time.
   *
   * @return The member heartbeat time.
   */
  long getHeartbeatTime() {
    return heartbeatTime;
  }

  /**
   * Sets the member heartbeat time.
   *
   * @param heartbeatTime The member heartbeat time.
   */
  void setHeartbeatTime(long heartbeatTime) {
    this.heartbeatTime = heartbeatTime;
  }

  /**
   * Returns the member heartbeat start time.
   *
   * @return The member heartbeat start time.
   */
  long getHeartbeatStartTime() {
    return heartbeatStartTime;
  }

  /**
   * Sets the member heartbeat start time.
   *
   * @param startTime The member heartbeat attempt start time.
   */
  void setHeartbeatStartTime(long startTime) {
    this.heartbeatStartTime = startTime;
  }

  /**
   * Returns the member failure count.
   *
   * @return The member failure count.
   */
  int getFailureCount() {
    return failures;
  }

  /**
   * Increments the member failure count.
   *
   * @return The member state.
   */
  int incrementFailureCount() {
    return ++failures;
  }

  /**
   * Resets the member failure count.
   */
  void resetFailureCount() {
    failures = 0;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("member", member.id())
        .add("term", term)
        .add("configIndex", configIndex)
        .add("nextSnapshotIndex", nextSnapshotIndex)
        .add("nextSnapshotOffset", nextSnapshotOffset)
        .add("matchIndex", matchIndex)
        .add("nextIndex", nextIndex)
        .add("heartbeatTime", heartbeatTime)
        .add("heartbeatStartTime", heartbeatStartTime)
        .add("appending", appending)
        .add("appendSucceeded", appendSucceeded)
        .add("appendTime", appendTime)
        .add("configuring", configuring)
        .add("installing", installing)
        .add("failures", failures)
        .toString();
  }

  /**
   * Timestamp ring buffer.
   */
  private static class TimeBuffer {
    private final long[] buffer;
    private int position;

    public TimeBuffer(int size) {
      this.buffer = new long[size];
    }

    /**
     * Records a request round trip time.
     *
     * @param time The request round trip time to record.
     */
    public void record(long time) {
      buffer[position++] = time;
      if (position >= buffer.length) {
        position = 0;
      }
    }

    /**
     * Returns the average of all recorded round trip times.
     *
     * @return The average of all recorded round trip times.
     */
    public long average() {
      long total = 0;
      for (long time : buffer) {
        if (time > 0) {
          total += time;
        }
      }
      return total / buffer.length;
    }

    /**
     * Resets the recorded round trip times.
     */
    public void reset() {
      for (int i = 0; i < buffer.length; i++) {
        buffer[i] = 0;
      }
      position = 0;
    }
  }

}
