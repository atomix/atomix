/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitives.leadership.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.atomix.primitives.leadership.Leader;
import io.atomix.primitives.leadership.Leadership;
import io.atomix.primitives.leadership.LeadershipEvent;
import io.atomix.primitives.leadership.LeadershipEvent.Type;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.Anoint;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.Evict;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.Promote;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.Run;
import io.atomix.protocols.raft.service.AbstractRaftService;
import io.atomix.protocols.raft.service.Commit;
import io.atomix.protocols.raft.service.RaftServiceExecutor;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.utils.ArraySizeHashPrinter;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.atomix.primitives.leadership.impl.RaftLeaderElectorEvents.CHANGE;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.ADD_LISTENER;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.ANOINT;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.EVICT;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.GET_LEADERSHIP;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.PROMOTE;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.REMOVE_LISTENER;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.RUN;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.WITHDRAW;

/**
 * State machine for {@link RaftLeaderElector} resource.
 */
public class RaftLeaderElectorService extends AbstractRaftService {

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
      .register(RaftLeaderElectorOperations.NAMESPACE)
      .register(RaftLeaderElectorEvents.NAMESPACE)
      .register(ElectionState.class)
      .register(Registration.class)
      .register(new LinkedHashMap<>().keySet().getClass())
      .build());

  private AtomicLong termCounter = new AtomicLong();
  private ElectionState election;
  private Map<Long, RaftSession> listeners = new LinkedHashMap<>();

  @Override
  public void snapshot(SnapshotWriter writer) {
    writer.writeLong(termCounter.get());
    writer.writeObject(election, SERIALIZER::encode);
    writer.writeObject(Sets.newHashSet(listeners.keySet()), SERIALIZER::encode);
    logger().debug("Took state machine snapshot");
  }

  @Override
  public void install(SnapshotReader reader) {
    termCounter.set(reader.readLong());
    election = reader.readObject(SERIALIZER::decode);
    listeners = new LinkedHashMap<>();
    for (Long sessionId : reader.<Set<Long>>readObject(SERIALIZER::decode)) {
      listeners.put(sessionId, sessions().getSession(sessionId));
    }
    logger().debug("Reinstated state machine from snapshot");
  }

  @Override
  protected void configure(RaftServiceExecutor executor) {
    // Notification
    executor.register(ADD_LISTENER, this::listen);
    executor.register(REMOVE_LISTENER, this::unlisten);
    // Commands
    executor.register(RUN, SERIALIZER::decode, this::run, SERIALIZER::encode);
    executor.register(WITHDRAW, this::withdraw);
    executor.register(ANOINT, SERIALIZER::decode, this::anoint, SERIALIZER::encode);
    executor.register(PROMOTE, SERIALIZER::decode, this::promote, SERIALIZER::encode);
    executor.register(EVICT, SERIALIZER::decode, this::evict);
    // Queries
    executor.register(GET_LEADERSHIP, this::getLeadership, SERIALIZER::encode);
  }

  private void notifyLeadershipChange(Leadership<byte[]> previousLeadership, Leadership<byte[]> newLeadership) {
    notifyLeadershipChanges(Lists.newArrayList(new LeadershipEvent<byte[]>(Type.CHANGE, previousLeadership, newLeadership)));
  }

  private void notifyLeadershipChanges(List<LeadershipEvent> changes) {
    if (changes.isEmpty()) {
      return;
    }
    listeners.values().forEach(session -> session.publish(CHANGE, SERIALIZER::encode, changes));
  }

  /**
   * Applies listen commits.
   *
   * @param commit listen commit
   */
  protected void listen(Commit<Void> commit) {
    listeners.put(commit.session().sessionId().id(), commit.session());
  }

  /**
   * Applies unlisten commits.
   *
   * @param commit unlisten commit
   */
  protected void unlisten(Commit<Void> commit) {
    listeners.remove(commit.session().sessionId().id());
  }

  /**
   * Applies an {@link RaftLeaderElectorOperations.Run} commit.
   *
   * @param commit commit entry
   * @return topic leader. If no previous leader existed this is the node that just entered the race.
   */
  protected Leadership<byte[]> run(Commit<? extends Run> commit) {
    try {
      Leadership<byte[]> oldLeadership = leadership();
      Registration registration = new Registration(commit.value().id(), commit.session().sessionId().id());
      if (election == null) {
        election = new ElectionState(registration, termCounter::incrementAndGet);
      } else if (!election.isDuplicate(registration)) {
        election = new ElectionState(election).addRegistration(registration, termCounter::incrementAndGet);
      }
      Leadership<byte[]> newLeadership = leadership();

      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
      return newLeadership;
    } catch (Exception e) {
      logger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies a withdraw commit.
   */
  protected void withdraw(Commit<Void> commit) {
    try {
      Leadership<byte[]> oldLeadership = leadership();
      if (election != null) {
        election = election.cleanup(commit.session(), termCounter::incrementAndGet);
      }
      Leadership<byte[]> newLeadership = leadership();
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
    } catch (Exception e) {
      logger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link RaftLeaderElectorOperations.Anoint} commit.
   *
   * @param commit anoint commit
   * @return {@code true} if changes were made and the transfer occurred; {@code false} if it did not.
   */
  protected boolean anoint(Commit<? extends Anoint> commit) {
    try {
      byte[] id = commit.value().id();
      Leadership<byte[]> oldLeadership = leadership();
      ElectionState electionState = election = election != null ? election.transferLeadership(id, termCounter) : null;
      Leadership<byte[]> newLeadership = leadership();
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
      return electionState != null
          && electionState.leader() != null
          && Arrays.equals(commit.value().id(), electionState.leader().id());
    } catch (Exception e) {
      logger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link RaftLeaderElectorOperations.Promote} commit.
   *
   * @param commit promote commit
   * @return {@code true} if changes desired end state is achieved.
   */
  protected boolean promote(Commit<? extends Promote> commit) {
    try {
      byte[] id = commit.value().id();
      Leadership<byte[]> oldLeadership = leadership();
      if (oldLeadership == null) {
        return false;
      } else {
        boolean containsCandidate = oldLeadership.candidates().stream()
            .anyMatch(a -> Arrays.equals(a, id));
        if (!containsCandidate) {
          return false;
        }
      }
      election = election.promote(id);
      Leadership<byte[]> newLeadership = leadership();
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
      return true;
    } catch (Exception e) {
      logger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link RaftLeaderElectorOperations.Evict} commit.
   *
   * @param commit evict commit
   */
  protected void evict(Commit<? extends Evict> commit) {
    try {
      byte[] id = commit.value().id();
      Leadership<byte[]> oldLeadership = leadership();
      election = election.evict(id, termCounter::incrementAndGet);
      Leadership<byte[]> newLeadership = leadership();
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
    } catch (Exception e) {
      logger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies a get leadership commit.
   *
   * @return leader
   */
  protected Leadership<byte[]> getLeadership() {
    try {
      return leadership();
    } catch (Exception e) {
      logger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  private Leadership<byte[]> leadership() {
    return election != null ? new Leadership<>(leader(), candidates()) : null;
  }

  private Leader<byte[]> leader() {
    return election == null ? null : election.leader();
  }

  private List<byte[]> candidates() {
    return election == null ? new LinkedList<>() : election.candidates();
  }

  private void onSessionEnd(RaftSession session) {
    listeners.remove(session.sessionId().id());
    Leadership<byte[]> oldLeadership = leadership();
    election = election.cleanup(session, termCounter::incrementAndGet);
    Leadership<byte[]> newLeadership = leadership();
    if (!Objects.equal(oldLeadership, newLeadership)) {
      notifyLeadershipChange(oldLeadership, newLeadership);
    }
  }

  private static class Registration {
    private final byte[] id;
    private final long sessionId;

    protected Registration(byte[] id, long sessionId) {
      this.id = id;
      this.sessionId = sessionId;
    }

    protected byte[] id() {
      return id;
    }

    protected long sessionId() {
      return sessionId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("id", ArraySizeHashPrinter.of(id))
          .add("sessionId", sessionId)
          .toString();
    }
  }

  private static class ElectionState {
    final Registration leader;
    final long term;
    final long termStartTime;
    final List<Registration> registrations;

    protected ElectionState(Registration registration, Supplier<Long> termCounter) {
      registrations = Arrays.asList(registration);
      term = termCounter.get();
      termStartTime = System.currentTimeMillis();
      leader = registration;
    }

    protected ElectionState(ElectionState other) {
      registrations = Lists.newArrayList(other.registrations);
      leader = other.leader;
      term = other.term;
      termStartTime = other.termStartTime;
    }

    protected ElectionState(List<Registration> registrations,
                         Registration leader,
                         long term,
                         long termStartTime) {
      this.registrations = Lists.newArrayList(registrations);
      this.leader = leader;
      this.term = term;
      this.termStartTime = termStartTime;
    }

    protected ElectionState cleanup(RaftSession session, Supplier<Long> termCounter) {
      Optional<Registration> registration =
          registrations.stream().filter(r -> r.sessionId() == session.sessionId().id()).findFirst();
      if (registration.isPresent()) {
        List<Registration> updatedRegistrations =
            registrations.stream()
                .filter(r -> r.sessionId() != session.sessionId().id())
                .collect(Collectors.toList());
        if (leader.sessionId() == session.sessionId().id()) {
          if (!updatedRegistrations.isEmpty()) {
            return new ElectionState(updatedRegistrations,
                updatedRegistrations.get(0),
                termCounter.get(),
                System.currentTimeMillis());
          } else {
            return new ElectionState(updatedRegistrations, null, term, termStartTime);
          }
        } else {
          return new ElectionState(updatedRegistrations, leader, term, termStartTime);
        }
      } else {
        return this;
      }
    }

    protected ElectionState evict(byte[] id, Supplier<Long> termCounter) {
      Optional<Registration> registration =
          registrations.stream().filter(r -> Arrays.equals(r.id, id)).findFirst();
      if (registration.isPresent()) {
        List<Registration> updatedRegistrations =
            registrations.stream()
                .filter(r -> !Arrays.equals(r.id(), id))
                .collect(Collectors.toList());
        if (Arrays.equals(leader.id(), id)) {
          if (!updatedRegistrations.isEmpty()) {
            return new ElectionState(updatedRegistrations,
                updatedRegistrations.get(0),
                termCounter.get(),
                System.currentTimeMillis());
          } else {
            return new ElectionState(updatedRegistrations, null, term, termStartTime);
          }
        } else {
          return new ElectionState(updatedRegistrations, leader, term, termStartTime);
        }
      } else {
        return this;
      }
    }

    protected boolean isDuplicate(Registration registration) {
      return registrations.stream().anyMatch(r -> r.sessionId() == registration.sessionId());
    }

    protected Leader<byte[]> leader() {
      if (leader == null) {
        return null;
      } else {
        byte[] leaderId = leader.id();
        return new Leader<>(leaderId, term, termStartTime);
      }
    }

    protected List<byte[]> candidates() {
      return registrations.stream().map(registration -> registration.id()).collect(Collectors.toList());
    }

    protected ElectionState addRegistration(Registration registration, Supplier<Long> termCounter) {
      if (!registrations.stream().anyMatch(r -> r.sessionId() == registration.sessionId())) {
        List<Registration> updatedRegistrations = new LinkedList<>(registrations);
        updatedRegistrations.add(registration);
        boolean newLeader = leader == null;
        return new ElectionState(updatedRegistrations,
            newLeader ? registration : leader,
            newLeader ? termCounter.get() : term,
            newLeader ? System.currentTimeMillis() : termStartTime);
      }
      return this;
    }

    protected ElectionState transferLeadership(byte[] id, AtomicLong termCounter) {
      Registration newLeader = registrations.stream()
          .filter(r -> Arrays.equals(r.id(), id))
          .findFirst()
          .orElse(null);
      if (newLeader != null) {
        return new ElectionState(registrations,
            newLeader,
            termCounter.incrementAndGet(),
            System.currentTimeMillis());
      } else {
        return this;
      }
    }

    protected ElectionState promote(byte[] id) {
      Registration registration = registrations.stream()
          .filter(r -> Arrays.equals(r.id(), id))
          .findFirst()
          .orElse(null);
      List<Registration> updatedRegistrations = Lists.newArrayList();
      updatedRegistrations.add(registration);
      registrations.stream()
          .filter(r -> !Arrays.equals(r.id(), id))
          .forEach(updatedRegistrations::add);
      return new ElectionState(updatedRegistrations,
          leader,
          term,
          termStartTime);

    }
  }

  @Override
  public void onExpire(RaftSession session) {
    onSessionEnd(session);
  }

  @Override
  public void onClose(RaftSession session) {
    onSessionEnd(session);
  }
}