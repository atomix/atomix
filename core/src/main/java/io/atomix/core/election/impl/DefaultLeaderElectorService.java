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
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.election.Leader;
import io.atomix.core.election.LeaderElectorType;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.misc.ArraySizeHashPrinter;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * State machine for {@link LeaderElectorProxy} resource.
 */
public class DefaultLeaderElectorService extends AbstractPrimitiveService<LeaderElectorClient> implements LeaderElectorService {

  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(LeaderElectorType.instance().namespace())
      .register(ElectionState.class)
      .register(Registration.class)
      .register(new LinkedHashMap<>().keySet().getClass())
      .register(SessionId.class)
      .build());

  private Map<String, AtomicLong> termCounters = new HashMap<>();
  private Map<String, ElectionState> elections = new HashMap<>();
  private Set<SessionId> listeners = new LinkedHashSet<>();

  public DefaultLeaderElectorService() {
    super(LeaderElectorType.instance(), LeaderElectorClient.class);
  }

  @Override
  public Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public void backup(BackupOutput writer) {
    writer.writeObject(listeners);
    writer.writeObject(termCounters);
    writer.writeObject(elections);
    getLogger().debug("Took state machine snapshot");
  }

  @Override
  public void restore(BackupInput reader) {
    listeners = reader.readObject();
    termCounters = reader.readObject();
    elections = reader.readObject();
    elections.values().forEach(e -> e.elections = elections);
    getLogger().debug("Reinstated state machine from snapshot");
  }

  private void notifyLeadershipChange(String topic, Leadership<byte[]> previousLeadership, Leadership<byte[]> newLeadership) {
    listeners.forEach(id -> getSession(id).accept(client -> client.onLeadershipChange(topic, previousLeadership, newLeadership)));
  }

  @Override
  public void listen() {
    listeners.add(getCurrentSession().sessionId());
  }

  @Override
  public void unlisten() {
    listeners.remove(getCurrentSession().sessionId());
  }

  @Override
  public Leadership<byte[]> run(String topic, byte[] id) {
    try {
      Leadership<byte[]> oldLeadership = leadership(topic);
      Registration registration = new Registration(id, getCurrentSession().sessionId().id());
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
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void withdraw(String topic, byte[] id) {
    try {
      Leadership<byte[]> oldLeadership = leadership(topic);
      elections.computeIfPresent(topic, (k, v) -> v.cleanup(
          topic, getCurrentSession(), termCounter(topic)::incrementAndGet));
      Leadership<byte[]> newLeadership = leadership(topic);
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(topic, oldLeadership, newLeadership);
      }
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean anoint(String topic, byte[] id) {
    try {
      Leadership<byte[]> oldLeadership = leadership(topic);
      ElectionState electionState = elections.computeIfPresent(topic,
          (k, v) -> v.transferLeadership(id, termCounter(topic)));
      Leadership<byte[]> newLeadership = leadership(topic);
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(topic, oldLeadership, newLeadership);
      }
      return (electionState != null
          && electionState.leader() != null
          && Arrays.equals(id, electionState.leader().id()));
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean promote(String topic, byte[] id) {
    try {
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
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void evict(byte[] id) {
    try {
      List<LeadershipEvent<byte[]>> changes = Lists.newArrayList();
      Set<String> topics = Maps.filterValues(elections, e -> e.candidates().stream().anyMatch(candidate -> Arrays.equals(candidate, id))).keySet();
      topics.forEach(topic -> {
        Leadership<byte[]> oldLeadership = leadership(topic);
        elections.compute(topic, (k, v) -> v.evict(id, termCounter(topic)::incrementAndGet));
        Leadership<byte[]> newLeadership = leadership(topic);
        if (!Objects.equal(oldLeadership, newLeadership)) {
          notifyLeadershipChange(topic, oldLeadership, newLeadership);
        }
      });
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Leadership<byte[]> getLeadership(String topic) {
    try {
      return leadership(topic);
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, Leadership<byte[]>> getLeaderships() {
    Map<String, Leadership<byte[]>> result = new HashMap<>();
    try {
      result.putAll(Maps.transformEntries(elections, (k, v) -> leadership(k)));
      return result;
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
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
    listeners.remove(session.sessionId());
    Set<String> topics = elections.keySet();
    topics.forEach(topic -> {
      Leadership<byte[]> oldLeadership = leadership(topic);
      elections.compute(topic, (k, v) -> v.cleanup(topic, session, termCounter(topic)::incrementAndGet));
      Leadership<byte[]> newLeadership = leadership(topic);
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(topic, oldLeadership, newLeadership);
      }
    });
  }

  private static class Registration {
    private final byte[] id;
    private final long sessionId;

    Registration(byte[] id, long sessionId) {
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

    ElectionState(Registration registration, Supplier<Long> termCounter,
                         Map<String, ElectionState> elections) {
      registrations = Arrays.asList(registration);
      term = termCounter.get();
      termStartTime = System.currentTimeMillis();
      leader = registration;
      this.elections = elections;
    }

    ElectionState(ElectionState other) {
      registrations = Lists.newArrayList(other.registrations);
      leader = other.leader;
      term = other.term;
      termStartTime = other.termStartTime;
      elections = other.elections;
    }

    ElectionState(List<Registration> registrations,
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
            //sortRegistrations(topic, updatedRegistrations);
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
        //sortRegistrations(topic, updatedRegistrations);
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
