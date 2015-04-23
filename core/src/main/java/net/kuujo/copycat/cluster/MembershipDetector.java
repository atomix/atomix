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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.ThreadChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Membership detector.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class MembershipDetector implements Runnable, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipDetector.class);
  private static final String MEMBERSHIP_TOPIC = "*";
  private static final long MEMBER_INFO_EXPIRE_TIME = 1000 * 60;
  private final AbstractCluster cluster;
  private final ExecutionContext context = new ExecutionContext("copycat-cluster");
  private final ThreadChecker threadChecker = new ThreadChecker(context);
  private final Random random = new Random();
  private final AtomicBoolean updating = new AtomicBoolean();

  MembershipDetector(AbstractCluster cluster) {
    this.cluster = cluster;
    context.execute(() -> {
      cluster.localMember.registerHandler(MEMBERSHIP_TOPIC, this::handleJoin);
    });
  }

  @Override
  public void run() {
    sendJoins(getGossipMembers());
  }

  /**
   * Sends member join requests to the given set of members.
   */
  private void sendJoins(Collection<AbstractMember> gossipMembers) {
    threadChecker.checkThread();

    // Increment the local member version.
    cluster.localMember.info.version(cluster.localMember.info.version() + 1);

    // For a random set of three members, send all member info.
    Collection<AbstractMember.Info> members = new ArrayList<>(cluster.members.values().stream().map(m -> m.info).collect(Collectors.toList()));
    for (AbstractMember member : gossipMembers) {
      if (member.id() != cluster.member().id()) {
        member.<Collection<AbstractMember.Info>, Collection<AbstractMember.Info>>send(MEMBERSHIP_TOPIC, members).whenCompleteAsync((membersInfo, error) -> {
          // If the response was successfully received then indicate that the member is alive and update all member info.
          // Otherwise, indicate that communication with the member failed. This information will be used to determine
          // whether the member should be considered dead by informing other members that it appears unreachable.
          if (cluster.isOpen()) {
            if (error == null) {
              member.info.succeed();
              updateMemberInfo(membersInfo);
            } else {
              member.info.fail(cluster.localMember.id());
            }
          }
        }, context);
      }
    }
  }

  /**
   * Receives member join requests.
   */
  private CompletableFuture<Collection<AbstractMember.Info>> handleJoin(Collection<AbstractMember.Info> members) {
    threadChecker.checkThread();
    // Increment the local member version.
    cluster.localMember.info.version(cluster.localMember.info.version() + 1);
    updateMemberInfo(members);
    return CompletableFuture.completedFuture(new ArrayList<>(cluster.members.values().stream().map(m -> m.info).collect(Collectors.toList())));
  }

  /**
   * Updates member info for all members.
   */
  private void updateMemberInfo(Collection<AbstractMember.Info> membersInfo) {
    threadChecker.checkThread();

    if (updating.compareAndSet(false, true)) {
      // Iterate through the member info and use it to update local member information.
      membersInfo.forEach(memberInfo -> {

        // If member info for the given URI is already present, update the member info based on versioning. Otherwise,
        // if the member info isn't already present then add it.
        AbstractMember matchMember = cluster.members.get(memberInfo.id());
        if (matchMember == null) {
          matchMember = cluster.createRemoteMember(memberInfo);
        } else {
          matchMember.info.update(memberInfo);
        }

        // Check whether the member info update should result in any member clients being added to or removed from the
        // cluster. If the updated member state is ALIVE or SUSPICIOUS, make sure the member client is open in the cluster.
        // Otherwise, if the updated member state is DEAD then make sure it has been removed from the cluster.
        final AbstractMember.Info updatedInfo = matchMember.info;
        if (updatedInfo.status() == Member.Status.ALIVE || updatedInfo.status() == Member.Status.SUSPICIOUS) {
          synchronized (cluster.members) {
            if (!cluster.members.containsKey(updatedInfo.id())) {
              AbstractRemoteMember member = cluster.createRemoteMember(updatedInfo);
              if (member != null) {
                member.connect().whenCompleteAsync((result, error) -> {
                  if (error == null) {
                    cluster.members.put(member.id(), member);
                    cluster.remoteMembers.put(member.id(), member);
                    LOGGER.info("{} - {} joined the cluster", cluster.localMember, member.id());
                    cluster.membershipListeners.forEach(listener -> listener.accept(new MembershipChangeEvent(MembershipChangeEvent.Type.JOIN, member)));
                    sendJoins(cluster.members.values());
                  }
                }, context);
              }
            }
          }
        } else {
          synchronized (cluster.members) {
            AbstractRemoteMember member = cluster.remoteMembers.remove(updatedInfo.id());
            if (member != null) {
              cluster.members.remove(member.id());
              member.close().whenComplete((result, error) -> {
                LOGGER.info("{} - {} left the cluster", cluster.localMember, member.id());
                cluster.membershipListeners.forEach(listener -> listener.accept(new MembershipChangeEvent(MembershipChangeEvent.Type.LEAVE, member)));
                sendJoins(cluster.members.values());
              });
            }
          }
        }
      });
      cleanMemberInfo();
    }
  }

  /**
   * Cleans expired member info for members that have been dead for MEMBER_INFO_EXPIRE_TIME milliseconds.
   */
  private synchronized void cleanMemberInfo() {
    threadChecker.checkThread();
    Iterator<Map.Entry<Integer, AbstractRemoteMember>> iterator = cluster.remoteMembers.entrySet().iterator();
    while (iterator.hasNext()) {
      AbstractMember member = iterator.next().getValue();
      if (member.status() == Member.Status.DEAD && System.currentTimeMillis() > member.info.changed() + MEMBER_INFO_EXPIRE_TIME) {
        iterator.remove();
        cluster.members.remove(member.id());
      }
    }
  }

  /**
   * Gets a list of members with which to gossip.
   */
  private Collection<AbstractMember> getGossipMembers() {
    try (Stream<AbstractMember> membersStream = cluster.members.values().stream();
         Stream<AbstractMember> activeStream = membersStream.filter(member -> member.id() != cluster.localMember.id()
           && (cluster.localMember.type() == Member.Type.ACTIVE && member.type() == Member.Type.PASSIVE)
           || (cluster.localMember.type() == Member.Type.PASSIVE && member.type() == Member.Type.ACTIVE)
           && (member.status() == Member.Status.SUSPICIOUS || member.status() == Member.Status.ALIVE))) {

      List<AbstractMember> activeMembers = activeStream.collect(Collectors.toList());

      // Create a random list of three active members.
      Collection<AbstractMember> randomMembers = new HashSet<>(3);
      for (int i = 0; i < Math.min(activeMembers.size(), 3); i++) {
        randomMembers.add(activeMembers.get(random.nextInt(Math.min(activeMembers.size(), 3))));
      }
      return randomMembers;
    }
  }

  @Override
  public void close() {
    context.execute(() -> {
      cluster.localMember.unregisterHandler(MEMBERSHIP_TOPIC);
    });
    context.close();
  }

}
