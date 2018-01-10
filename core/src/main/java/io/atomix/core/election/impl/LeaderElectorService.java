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
package io.atomix.core.election.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.atomix.core.election.Leader;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEvent.Type;
import io.atomix.core.election.impl.LeaderElectorOperations.Anoint;
import io.atomix.core.election.impl.LeaderElectorOperations.Evict;
import io.atomix.core.election.impl.LeaderElectorOperations.GetElectedTopics;
import io.atomix.core.election.impl.LeaderElectorOperations.GetLeadership;
import io.atomix.core.election.impl.LeaderElectorOperations.Promote;
import io.atomix.core.election.impl.LeaderElectorOperations.Run;
import io.atomix.core.election.impl.LeaderElectorOperations.Withdraw;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.utils.ArraySizeHashPrinter;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;

import static io.atomix.core.election.impl.LeaderElectorEvents.CHANGE;
import static io.atomix.core.election.impl.LeaderElectorOperations.ADD_LISTENER;
import static io.atomix.core.election.impl.LeaderElectorOperations.ANOINT;
import static io.atomix.core.election.impl.LeaderElectorOperations.EVICT;
import static io.atomix.core.election.impl.LeaderElectorOperations.GET_ALL_LEADERSHIPS;
import static io.atomix.core.election.impl.LeaderElectorOperations.GET_ELECTED_TOPICS;
import static io.atomix.core.election.impl.LeaderElectorOperations.GET_LEADERSHIP;
import static io.atomix.core.election.impl.LeaderElectorOperations.PROMOTE;
import static io.atomix.core.election.impl.LeaderElectorOperations.REMOVE_LISTENER;
import static io.atomix.core.election.impl.LeaderElectorOperations.RUN;
import static io.atomix.core.election.impl.LeaderElectorOperations.WITHDRAW;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * State machine for {@link LeaderElectorProxy} resource.
 */
public class LeaderElectorService extends AbstractPrimitiveService {

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(LeaderElectorOperations.NAMESPACE)
      .register(LeaderElectorEvents.NAMESPACE)
      .register(ElectionState.class)
      .register(Registration.class)
      .register(new LinkedHashMap<>().keySet().getClass())
      .build());

  private Map<String, AtomicLong> termCounters = new HashMap<>();
  private Map<String, ElectionState> elections = new HashMap<>();
  private Map<Long, Session> listeners = new LinkedHashMap<>();

  @Override
  public void backup(BufferOutput<?> writer) {
    writer.writeObject(Sets.newHashSet(listeners.keySet()), SERIALIZER::encode);
    writer.writeObject(termCounters, SERIALIZER::encode);
    writer.writeObject(elections, SERIALIZER::encode);
    getLogger().debug("Took state machine snapshot");
  }

  @Override
  public void restore(BufferInput<?> reader) {
    listeners = new LinkedHashMap<>();
    for (Long sessionId : reader.<Set<Long>>readObject(SERIALIZER::decode)) {
      listeners.put(sessionId, getSessions().getSession(sessionId));
    }
    termCounters = reader.readObject(SERIALIZER::decode);
    elections = reader.readObject(SERIALIZER::decode);
    elections.values().forEach(e -> e.elections = elections);
    getLogger().debug("Reinstated state machine from snapshot");
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    // Notification
    executor.register(ADD_LISTENER, this::listen);
    executor.register(REMOVE_LISTENER, this::unlisten);
    // Commands
    executor.register(RUN, SERIALIZER::decode, this::run, SERIALIZER::encode);
    executor.register(WITHDRAW, SERIALIZER::decode, this::withdraw);
    executor.register(ANOINT, SERIALIZER::decode, this::anoint, SERIALIZER::encode);
    executor.register(PROMOTE, SERIALIZER::decode, this::promote, SERIALIZER::encode);
    executor.register(EVICT, SERIALIZER::decode, this::evict);
    // Queries
    executor.register(GET_LEADERSHIP, SERIALIZER::decode, this::getLeadership, SERIALIZER::encode);
    executor.register(GET_ALL_LEADERSHIPS, this::allLeaderships, SERIALIZER::encode);
    executor.register(GET_ELECTED_TOPICS, SERIALIZER::decode, this::electedTopics, SERIALIZER::encode);
  }

  private void notifyLeadershipChange(String topic, Leadership<byte[]> previousLeadership, Leadership<byte[]> newLeadership) {
    notifyLeadershipChanges(Lists.newArrayList(new LeadershipEvent<>(Type.CHANGE, topic, previousLeadership, newLeadership)));
  }

  private void notifyLeadershipChanges(List<LeadershipEvent<byte[]>> changes) {
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
  public void listen(Commit<Void> commit) {
    listeners.put(commit.session().sessionId().id(), commit.session());
  }

  /**
   * Applies unlisten commits.
   *
   * @param commit unlisten commit
   */
  public void unlisten(Commit<Void> commit) {
    listeners.remove(commit.session().sessionId().id());
  }

  /**
   * Applies an {@link LeaderElectorOperations.Run} commit.
   *
   * @param commit commit entry
   * @return topic leader. If no previous leader existed this is the node that just entered the race.
   */
  public Leadership run(Commit<? extends Run> commit) {
    try {
      String topic = commit.value().topic();
      Leadership<byte[]> oldLeadership = leadership(topic);
      Registration registration = new Registration(commit.value().id(), commit.session().sessionId().id());
      elections.compute(topic, (k, v) -> {
        if (v == null) {
          return new ElectionState(registration, termCounter(topic)::incrementAndGet, elections);
        } else {
          if (!v.isDuplicate(registration)) {
            return new ElectionState(v).addRegistration(
                topic, registration, termCounter(topic)::incrementAndGet);
          } else {
            return v;
          }
        }
      });
      Leadership<byte[]> newLeadership = leadership(topic);

      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(topic, oldLeadership, newLeadership);
      }
      return newLeadership;
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link LeaderElectorOperations.Withdraw} commit.
   *
   * @param commit withdraw commit
   */
  public void withdraw(Commit<? extends Withdraw> commit) {
    try {
      String topic = commit.value().topic();
      Leadership<byte[]> oldLeadership = leadership(topic);
      elections.computeIfPresent(topic, (k, v) -> v.cleanup(
          topic, commit.session(), termCounter(topic)::incrementAndGet));
      Leadership<byte[]> newLeadership = leadership(topic);
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(topic, oldLeadership, newLeadership);
      }
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link LeaderElectorOperations.Anoint} commit.
   *
   * @param commit anoint commit
   * @return {@code true} if changes were made and the transfer occurred; {@code false} if it did not.
   */
  public boolean anoint(Commit<? extends Anoint> commit) {
    try {
      String topic = commit.value().topic();
      byte[] id = commit.value().id();
      Leadership<byte[]> oldLeadership = leadership(topic);
      ElectionState electionState = elections.computeIfPresent(topic,
          (k, v) -> v.transferLeadership(id, termCounter(topic)));
      Leadership<byte[]> newLeadership = leadership(topic);
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(topic, oldLeadership, newLeadership);
      }
      return (electionState != null &&
          electionState.leader() != null &&
          Arrays.equals(commit.value().id(), electionState.leader().id()));
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link LeaderElectorOperations.Promote} commit.
   *
   * @param commit promote commit
   * @return {@code true} if changes desired end state is achieved.
   */
  public boolean promote(Commit<? extends Promote> commit) {
    try {
      String topic = commit.value().topic();
      byte[] id = commit.value().id();
      Leadership<byte[]> oldLeadership = leadership(topic);
      if (oldLeadership == null || oldLeadership.candidates().stream().noneMatch(candidate -> Arrays.equals(candidate, id))) {
        return false;
      }
      elections.computeIfPresent(topic, (k, v) -> v.promote(id));
      Leadership<byte[]> newLeadership = leadership(topic);
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(topic, oldLeadership, newLeadership);
      }
      return true;
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link LeaderElectorOperations.Evict} commit.
   *
   * @param commit evict commit
   */
  public void evict(Commit<? extends Evict> commit) {
    try {
      List<LeadershipEvent<byte[]>> changes = Lists.newArrayList();
      byte[] id = commit.value().id();
      Set<String> topics = Maps.filterValues(elections, e -> e.candidates().stream().anyMatch(candidate -> Arrays.equals(candidate, id))).keySet();
      topics.forEach(topic -> {
        Leadership<byte[]> oldLeadership = leadership(topic);
        elections.compute(topic, (k, v) -> v.evict(id, termCounter(topic)::incrementAndGet));
        Leadership<byte[]> newLeadership = leadership(topic);
        if (!Objects.equal(oldLeadership, newLeadership)) {
          changes.add(new LeadershipEvent<>(Type.CHANGE, topic, oldLeadership, newLeadership));
        }
      });
      notifyLeadershipChanges(changes);
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link LeaderElectorOperations.GetLeadership} commit.
   *
   * @param commit GetLeadership commit
   * @return leader
   */
  public Leadership getLeadership(Commit<? extends GetLeadership> commit) {
    String topic = commit.value().topic();
    try {
      return leadership(topic);
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link LeaderElectorOperations.GetElectedTopics} commit.
   *
   * @param commit commit entry
   * @return set of topics for which the node is the leader
   */
  public Set<String> electedTopics(Commit<? extends GetElectedTopics> commit) {
    try {
      byte[] id = commit.value().id();
      return ImmutableSet.copyOf(Maps.filterEntries(elections, e -> {
        Leader<byte[]> leader = leadership(e.getKey()).leader();
        return leader != null && Arrays.equals(leader.id(), id);
      }).keySet());
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link LeaderElectorOperations#GET_ALL_LEADERSHIPS} commit.
   *
   * @param commit GetAllLeaderships commit
   * @return topic to leader mapping
   */
  public Map<String, Leadership> allLeaderships(Commit<Void> commit) {
    Map<String, Leadership> result = new HashMap<>();
    try {
      result.putAll(Maps.transformEntries(elections, (k, v) -> leadership(k)));
      return result;
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  private Leadership<byte[]> leadership(String topic) {
    return new Leadership<>(leader(topic), candidates(topic));
  }

  private Leader<byte[]> leader(String topic) {
    ElectionState electionState = elections.get(topic);
    return electionState == null ? null : electionState.leader();
  }

  private List<byte[]> candidates(String topic) {
    ElectionState electionState = elections.get(topic);
    return electionState == null ? new LinkedList<>() : electionState.candidates();
  }

  private void onSessionEnd(Session session) {
    listeners.remove(session.sessionId().id());
    Set<String> topics = elections.keySet();
    List<LeadershipEvent<byte[]>> changes = Lists.newArrayList();
    topics.forEach(topic -> {
      Leadership<byte[]> oldLeadership = leadership(topic);
      elections.compute(topic, (k, v) -> v.cleanup(topic, session, termCounter(topic)::incrementAndGet));
      Leadership<byte[]> newLeadership = leadership(topic);
      if (!Objects.equal(oldLeadership, newLeadership)) {
        changes.add(new LeadershipEvent<>(Type.CHANGE, topic, oldLeadership, newLeadership));
      }
    });
    notifyLeadershipChanges(changes);
  }

  private static class Registration {
    private final byte[] id;
    private final long sessionId;

    public Registration(byte[] id, long sessionId) {
      this.id = id;
      this.sessionId = sessionId;
    }

    public byte[] id() {
      return id;
    }

    public long sessionId() {
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

  private class ElectionState {
    final Registration leader;
    final long term;
    final long termStartTime;
    final List<Registration> registrations;
    transient Map<String, ElectionState> elections;

    public ElectionState(Registration registration, Supplier<Long> termCounter,
                         Map<String, ElectionState> elections) {
      registrations = Arrays.asList(registration);
      term = termCounter.get();
      termStartTime = System.currentTimeMillis();
      leader = registration;
      this.elections = elections;
    }

    public ElectionState(ElectionState other) {
      registrations = Lists.newArrayList(other.registrations);
      leader = other.leader;
      term = other.term;
      termStartTime = other.termStartTime;
      elections = other.elections;
    }

    public ElectionState(List<Registration> registrations,
                         Registration leader,
                         long term,
                         long termStartTime,
                         Map<String, ElectionState> elections) {
      this.registrations = Lists.newArrayList(registrations);
      this.leader = leader;
      this.term = term;
      this.termStartTime = termStartTime;
      this.elections = elections;
    }

    private void sortRegistrations(String topic, List<Registration> registrations) {
      registrations.sort((a, b) -> ComparisonChain.start()
          .compare(countLeaders(topic, a), countLeaders(topic, b))
          .compare(a.sessionId, b.sessionId)
          .result());
    }

    private long countLeaders(String topic, Registration registration) {
      return elections.entrySet().stream()
          .filter(entry -> !entry.getKey().equals(topic))
          .filter(entry -> entry.getValue().leader != null)
          .filter(entry -> {
            // Get the topic leader's identifier and a list of session identifiers.
            // Then return true if the leader's identifier matches any of the session's candidates.
            byte[] leaderId = entry.getValue().leader().id();
            List<byte[]> sessionCandidates = entry.getValue().registrations.stream()
                .filter(r -> r.sessionId == registration.sessionId)
                .map(r -> r.id)
                .collect(Collectors.toList());
            return sessionCandidates.stream()
                .anyMatch(candidate -> Arrays.equals(candidate, leaderId));
          })
          .count();
    }

    public ElectionState cleanup(String topic, Session session, Supplier<Long> termCounter) {
      Optional<Registration> registration =
          registrations.stream().filter(r -> r.sessionId() == session.sessionId().id()).findFirst();
      if (registration.isPresent()) {
        List<Registration> updatedRegistrations =
            registrations.stream()
                .filter(r -> r.sessionId() != session.sessionId().id())
                .collect(Collectors.toList());
        if (leader.sessionId() == session.sessionId().id()) {
          if (!updatedRegistrations.isEmpty()) {
            sortRegistrations(topic, updatedRegistrations);
            return new ElectionState(updatedRegistrations,
                updatedRegistrations.get(0),
                termCounter.get(),
                System.currentTimeMillis(),
                elections);
          } else {
            return new ElectionState(updatedRegistrations, null, term, termStartTime, elections);
          }
        } else {
          return new ElectionState(updatedRegistrations, leader, term, termStartTime, elections);
        }
      } else {
        return this;
      }
    }

    public ElectionState evict(byte[] id, Supplier<Long> termCounter) {
      Optional<Registration> registration =
          registrations.stream().filter(r -> Arrays.equals(r.id(), id)).findFirst();
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
                System.currentTimeMillis(),
                elections);
          } else {
            return new ElectionState(updatedRegistrations, null, term, termStartTime, elections);
          }
        } else {
          return new ElectionState(updatedRegistrations, leader, term, termStartTime, elections);
        }
      } else {
        return this;
      }
    }

    public boolean isDuplicate(Registration registration) {
      return registrations.stream().anyMatch(r -> r.sessionId() == registration.sessionId());
    }

    public Leader<byte[]> leader() {
      if (leader == null) {
        return null;
      } else {
        byte[] leaderId = leader.id();
        return new Leader<>(leaderId, term, termStartTime);
      }
    }

    public List<byte[]> candidates() {
      return registrations.stream().map(registration -> registration.id()).collect(Collectors.toList());
    }

    public ElectionState addRegistration(String topic, Registration registration, Supplier<Long> termCounter) {
      if (!registrations.stream().anyMatch(r -> r.sessionId() == registration.sessionId())) {
        List<Registration> updatedRegistrations = new LinkedList<>(registrations);
        updatedRegistrations.add(registration);
        sortRegistrations(topic, updatedRegistrations);
        Registration firstRegistration = updatedRegistrations.get(0);
        Registration leader = this.leader;
        long term = this.term;
        long termStartTime = this.termStartTime;
        if (leader == null || !leader.equals(firstRegistration)) {
          leader = firstRegistration;
          term = termCounter.get();
          termStartTime = System.currentTimeMillis();
        }
        return new ElectionState(updatedRegistrations,
            leader,
            term,
            termStartTime,
            elections);
      }
      return this;
    }

    public ElectionState transferLeadership(byte[] id, AtomicLong termCounter) {
      Registration newLeader = registrations.stream()
          .filter(r -> Arrays.equals(r.id(), id))
          .findFirst()
          .orElse(null);
      if (newLeader != null) {
        return new ElectionState(registrations,
            newLeader,
            termCounter.incrementAndGet(),
            System.currentTimeMillis(),
            elections);
      } else {
        return this;
      }
    }

    public ElectionState promote(byte[] id) {
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
          termStartTime,
          elections);

    }
  }

  @Override
  public void onExpire(Session session) {
    onSessionEnd(session);
  }

  @Override
  public void onClose(Session session) {
    onSessionEnd(session);
  }

  private AtomicLong termCounter(String topic) {
    return termCounters.computeIfAbsent(topic, k -> new AtomicLong(0));
  }
}