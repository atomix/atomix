/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.cluster.impl;

import io.atomix.protocols.raft.storage.log.RaftLog;
import io.atomix.protocols.raft.storage.log.RaftLogReader;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster member state.
 */
public final class RaftMemberContext {
  private static final int MAX_APPENDS = 2;
  private static final int APPEND_WINDOW_SIZE = 8;
  private final DefaultRaftMember member;
  private long term;
  private long configIndex;
  private long snapshotIndex;
  private long nextSnapshotIndex;
  private int nextSnapshotOffset;
  private long matchIndex;
  private long heartbeatTime;
  private long responseTime;
  private int appending;
  private boolean appendSucceeded;
  private long appendTime;
  private boolean configuring;
  private boolean installing;
  private int failures;
  private long failureTime;
  private volatile RaftLogReader reader;
  private final DescriptiveStatistics timeStats = new DescriptiveStatistics(APPEND_WINDOW_SIZE);

  RaftMemberContext(DefaultRaftMember member, RaftClusterContext cluster) {
    this.member = checkNotNull(member, "member cannot be null").setCluster(cluster);
  }

  /**
   * Resets the member state.
   */
  public void resetState(RaftLog log) {
    snapshotIndex = 0;
    nextSnapshotIndex = 0;
    nextSnapshotOffset = 0;
    matchIndex = 0;
    heartbeatTime = 0;
    responseTime = 0;
    appending = 0;
    timeStats.clear();
    configuring = false;
    installing = false;
    appendSucceeded = false;
    failures = 0;
    failureTime = 0;

    switch (member.getType()) {
      case PASSIVE:
        reader = log.openReader(log.writer().getLastIndex() + 1, RaftLogReader.Mode.COMMITS);
        break;
      case PROMOTABLE:
      case ACTIVE:
        reader = log.openReader(log.writer().getLastIndex() + 1, RaftLogReader.Mode.ALL);
        break;
    }
  }

  /**
   * Returns the member.
   *
   * @return The member.
   */
  public DefaultRaftMember getMember() {
    return member;
  }

  /**
   * Returns the member log reader.
   *
   * @return The member log reader.
   */
  public RaftLogReader getLogReader() {
    return reader;
  }

  /**
   * Returns the member term.
   *
   * @return The member term.
   */
  public long getConfigTerm() {
    return term;
  }

  /**
   * Sets the member term.
   *
   * @param term The member term.
   */
  public void setConfigTerm(long term) {
    this.term = term;
  }

  /**
   * Returns the member configuration index.
   *
   * @return The member configuration index.
   */
  public long getConfigIndex() {
    return configIndex;
  }

  /**
   * Sets the member configuration index.
   *
   * @param configIndex The member configuration index.
   */
  public void setConfigIndex(long configIndex) {
    this.configIndex = configIndex;
  }

  /**
   * Returns the member's current snapshot index.
   *
   * @return The member's current snapshot index.
   */
  public long getSnapshotIndex() {
    return snapshotIndex;
  }

  /**
   * Sets the member's current snapshot index.
   *
   * @param snapshotIndex The member's current snapshot index.
   */
  public void setSnapshotIndex(long snapshotIndex) {
    this.snapshotIndex = snapshotIndex;
  }

  /**
   * Returns the member's next snapshot index.
   *
   * @return The member's next snapshot index.
   */
  public long getNextSnapshotIndex() {
    return nextSnapshotIndex;
  }

  /**
   * Sets the member's next snapshot index.
   *
   * @param nextSnapshotIndex The member's next snapshot index.
   */
  public void setNextSnapshotIndex(long nextSnapshotIndex) {
    this.nextSnapshotIndex = nextSnapshotIndex;
  }

  /**
   * Returns the member's snapshot offset.
   *
   * @return The member's snapshot offset.
   */
  public int getNextSnapshotOffset() {
    return nextSnapshotOffset;
  }

  /**
   * Sets the member's snapshot offset.
   *
   * @param nextSnapshotOffset The member's snapshot offset.
   */
  public void setNextSnapshotOffset(int nextSnapshotOffset) {
    this.nextSnapshotOffset = nextSnapshotOffset;
  }

  /**
   * Returns the member's match index.
   *
   * @return The member's match index.
   */
  public long getMatchIndex() {
    return matchIndex;
  }

  /**
   * Sets the member's match index.
   *
   * @param matchIndex The member's match index.
   */
  public void setMatchIndex(long matchIndex) {
    checkArgument(matchIndex >= 0, "matchIndex must be positive");
    this.matchIndex = matchIndex;
  }

  /**
   * Returns a boolean indicating whether an append request can be sent to the member.
   *
   * @return Indicates whether an append request can be sent to the member.
   */
  public boolean canAppend() {
    return appending == 0 || (appendSucceeded && appending < MAX_APPENDS && System.currentTimeMillis() - (timeStats.getMean() / MAX_APPENDS) >= appendTime);
  }

  /**
   * Returns whether a heartbeat can be sent to the member.
   *
   * @return Indicates whether a heartbeat can be sent to the member.
   */
  public boolean canHeartbeat() {
    return appending == 0;
  }

  /**
   * Flags the last append to the member as successful.
   */
  public void appendSucceeded() {
    appendSucceeded(true);
  }

  /**
   * Flags the last append to the member is failed.
   */
  public void appendFailed() {
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
  public void startAppend() {
    appending++;
    appendTime = System.currentTimeMillis();
  }

  /**
   * Completes an append request to the member.
   */
  public void completeAppend() {
    appending--;
  }

  /**
   * Completes an append request to the member.
   *
   * @param time The time in milliseconds for the append.
   */
  public void completeAppend(long time) {
    appending--;
    timeStats.addValue(time);
  }

  /**
   * Returns a boolean indicating whether a configure request can be sent to the member.
   *
   * @return Indicates whether a configure request can be sent to the member.
   */
  public boolean canConfigure() {
    return !configuring;
  }

  /**
   * Starts a configure request to the member.
   */
  public void startConfigure() {
    configuring = true;
  }

  /**
   * Completes a configure request to the member.
   */
  public void completeConfigure() {
    configuring = false;
  }

  /**
   * Returns a boolean indicating whether an install request can be sent to the member.
   *
   * @return Indicates whether an install request can be sent to the member.
   */
  public boolean canInstall() {
    return !installing;
  }

  /**
   * Starts an install request to the member.
   */
  public void startInstall() {
    installing = true;
  }

  /**
   * Completes an install request to the member.
   */
  public void completeInstall() {
    installing = false;
  }

  /**
   * Returns the member heartbeat time.
   *
   * @return The member heartbeat time.
   */
  public long getHeartbeatTime() {
    return heartbeatTime;
  }

  /**
   * Sets the member heartbeat time.
   *
   * @param heartbeatTime The member heartbeat time.
   */
  public void setHeartbeatTime(long heartbeatTime) {
    this.heartbeatTime = Math.max(this.heartbeatTime, heartbeatTime);
  }

  /**
   * Returns the member response time.
   *
   * @return The member response time.
   */
  public long getResponseTime() {
    return responseTime;
  }

  /**
   * Sets the member response time.
   *
   * @param heartbeatTime The member response time.
   */
  public void setResponseTime(long responseTime) {
    this.responseTime = Math.max(this.responseTime, responseTime);
  }

  /**
   * Returns the member failure count.
   *
   * @return The member failure count.
   */
  public int getFailureCount() {
    return failures;
  }

  /**
   * Increments the member failure count.
   *
   * @return The member state.
   */
  public int incrementFailureCount() {
    if (failures++ == 0) {
      failureTime = System.currentTimeMillis();
    }
    return failures;
  }

  /**
   * Resets the member failure count.
   */
  public void resetFailureCount() {
    failures = 0;
    failureTime = 0;
  }

  /**
   * Returns the member failure time.
   *
   * @return the member failure time
   */
  public long getFailureTime() {
    return failureTime;
  }

  @Override
  public String toString() {
    RaftLogReader reader = this.reader;
    return toStringHelper(this)
        .add("member", member.memberId())
        .add("term", term)
        .add("configIndex", configIndex)
        .add("snapshotIndex", snapshotIndex)
        .add("nextSnapshotIndex", nextSnapshotIndex)
        .add("nextSnapshotOffset", nextSnapshotOffset)
        .add("matchIndex", matchIndex)
        .add("nextIndex", reader != null ? reader.getNextIndex() : matchIndex + 1)
        .add("heartbeatTime", heartbeatTime)
        .add("appending", appending)
        .add("appendSucceeded", appendSucceeded)
        .add("appendTime", appendTime)
        .add("configuring", configuring)
        .add("installing", installing)
        .add("failures", failures)
        .toString();
  }

}
