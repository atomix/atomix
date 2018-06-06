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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.atomix.core.election.Leader;
import io.atomix.core.election.LeaderElectionType;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEvent.Type;
import io.atomix.core.election.impl.LeaderElectionOperations.Anoint;
import io.atomix.core.election.impl.LeaderElectionOperations.Evict;
import io.atomix.core.election.impl.LeaderElectionOperations.Promote;
import io.atomix.core.election.impl.LeaderElectionOperations.Run;
import io.atomix.core.election.impl.LeaderElectionOperations.Withdraw;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.PrimitiveSession;
import io.atomix.utils.misc.ArraySizeHashPrinter;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.atomix.core.election.impl.LeaderElectionEvents.CHANGE;
import static io.atomix.core.election.impl.LeaderElectionOperations.ADD_LISTENER;
import static io.atomix.core.election.impl.LeaderElectionOperations.ANOINT;
import static io.atomix.core.election.impl.LeaderElectionOperations.EVICT;
import static io.atomix.core.election.impl.LeaderElectionOperations.GET_LEADERSHIP;
import static io.atomix.core.election.impl.LeaderElectionOperations.PROMOTE;
import static io.atomix.core.election.impl.LeaderElectionOperations.REMOVE_LISTENER;
import static io.atomix.core.election.impl.LeaderElectionOperations.RUN;
import static io.atomix.core.election.impl.LeaderElectionOperations.WITHDRAW;

/**
 * State machine for {@link LeaderElectionProxy} resource.
 */
public class LeaderElectionService extends AbstractPrimitiveService {

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(LeaderElectionOperations.NAMESPACE)
      .register(LeaderElectionEvents.NAMESPACE)
      .register(Registration.class)
      .register(new LinkedHashMap<>().keySet().getClass())
      .build());

  private Registration leader;
  private long term;
  private long termStartTime;
  private List<Registration> registrations = new LinkedList<>();
  private AtomicLong termCounter = new AtomicLong();
  private Map<Long, PrimitiveSession> listeners = new LinkedHashMap<>();

  public LeaderElectionService() {
    super(LeaderElectionType.instance());
  }

  @Override
  public Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public void backup(BackupOutput writer) {
    writer.writeLong(termCounter.get());
    writer.writeObject(leader);
    writer.writeLong(term);
    writer.writeLong(termStartTime);
    writer.writeObject(registrations);
    writer.writeObject(Sets.newHashSet(listeners.keySet()));
    getLogger().debug("Took state machine snapshot");
  }

  @Override
  public void restore(BackupInput reader) {
    termCounter.set(reader.readLong());
    leader = reader.readObject();
    term = reader.readLong();
    termStartTime = reader.readLong();
    registrations = reader.readObject();
    listeners = new LinkedHashMap<>();
    for (Long sessionId : reader.<Set<Long>>readObject()) {
      listeners.put(sessionId, getSession(sessionId));
    }
    getLogger().debug("Reinstated state machine from snapshot");
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    // Notification
    executor.register(ADD_LISTENER, this::listen);
    executor.register(REMOVE_LISTENER, this::unlisten);
    // Commands
    executor.register(RUN, this::run);
    executor.register(WITHDRAW, this::withdraw);
    executor.register(ANOINT, this::anoint);
    executor.register(PROMOTE, this::promote);
    executor.register(EVICT, this::evict);
    // Queries
    executor.register(GET_LEADERSHIP, this::getLeadership);
  }

  private void notifyLeadershipChange(Leadership<byte[]> previousLeadership, Leadership<byte[]> newLeadership) {
    notifyLeadershipChanges(Lists.newArrayList(new LeadershipEvent<>(Type.CHANGE, null, previousLeadership, newLeadership)));
  }

  private void notifyLeadershipChanges(List<LeadershipEvent> changes) {
    if (changes.isEmpty()) {
      return;
    }
    listeners.values().forEach(session -> session.publish(CHANGE, changes));
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
   * Applies an {@link LeaderElectionOperations.Run} commit.
   *
   * @param commit commit entry
   * @return topic leader. If no previous leader existed this is the node that just entered the race.
   */
  protected Leadership<byte[]> run(Commit<? extends Run> commit) {
    try {
      Leadership<byte[]> oldLeadership = leadership();
      Registration registration = new Registration(commit.value().id(), commit.session().sessionId().id());
      addRegistration(registration);
      Leadership<byte[]> newLeadership = leadership();

      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
      return newLeadership;
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies a withdraw commit.
   */
  protected void withdraw(Commit<? extends Withdraw> commit) {
    try {
      Leadership<byte[]> oldLeadership = leadership();
      cleanup(commit.value().id());
      Leadership<byte[]> newLeadership = leadership();
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link LeaderElectionOperations.Anoint} commit.
   *
   * @param commit anoint commit
   * @return {@code true} if changes were made and the transfer occurred; {@code false} if it did not.
   */
  protected boolean anoint(Commit<? extends Anoint> commit) {
    try {
      byte[] id = commit.value().id();
      Leadership<byte[]> oldLeadership = leadership();
      Registration newLeader = registrations.stream()
          .filter(r -> Arrays.equals(r.id(), id))
          .findFirst()
          .orElse(null);
      if (newLeader != null) {
        this.leader = newLeader;
        this.term = termCounter.incrementAndGet();
        this.termStartTime = getWallClock().getTime().unixTimestamp();
      }
      Leadership<byte[]> newLeadership = leadership();
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
      return leader != null && Arrays.equals(commit.value().id(), leader.id());
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link LeaderElectionOperations.Promote} commit.
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
      Registration registration = registrations.stream()
          .filter(r -> Arrays.equals(r.id(), id))
          .findFirst()
          .orElse(null);
      List<Registration> updatedRegistrations = Lists.newArrayList();
      updatedRegistrations.add(registration);
      registrations.stream()
          .filter(r -> !Arrays.equals(r.id(), id))
          .forEach(updatedRegistrations::add);
      this.registrations = updatedRegistrations;
      Leadership<byte[]> newLeadership = leadership();
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
      return true;
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Applies an {@link LeaderElectionOperations.Evict} commit.
   *
   * @param commit evict commit
   */
  protected void evict(Commit<? extends Evict> commit) {
    try {
      byte[] id = commit.value().id();
      Leadership<byte[]> oldLeadership = leadership();
      Optional<Registration> registration =
          registrations.stream().filter(r -> Arrays.equals(r.id, id)).findFirst();
      if (registration.isPresent()) {
        List<Registration> updatedRegistrations =
            registrations.stream()
                .filter(r -> !Arrays.equals(r.id(), id))
                .collect(Collectors.toList());
        if (Arrays.equals(leader.id(), id)) {
          if (!updatedRegistrations.isEmpty()) {
            this.registrations = updatedRegistrations;
            this.leader = updatedRegistrations.get(0);
            this.term = termCounter.incrementAndGet();
            this.termStartTime = getWallClock().getTime().unixTimestamp();
          } else {
            this.registrations = updatedRegistrations;
            this.leader = null;
          }
        } else {
          this.registrations = updatedRegistrations;
        }
      }
      Leadership<byte[]> newLeadership = leadership();
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
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
      getLogger().error("State machine operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  private Leadership<byte[]> leadership() {
    return new Leadership<>(leader(), candidates());
  }

  private void onSessionEnd(PrimitiveSession session) {
    listeners.remove(session.sessionId().id());
    Leadership<byte[]> oldLeadership = leadership();
    cleanup(session);
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

  protected void cleanup(byte[] id) {
    Optional<Registration> registration =
        registrations.stream().filter(r -> Arrays.equals(r.id(), id)).findFirst();
    if (registration.isPresent()) {
      List<Registration> updatedRegistrations =
          registrations.stream()
              .filter(r -> !Arrays.equals(r.id(), id))
              .collect(Collectors.toList());
      if (Arrays.equals(leader.id(), id)) {
        if (!updatedRegistrations.isEmpty()) {
          this.registrations = updatedRegistrations;
          this.leader = updatedRegistrations.get(0);
          this.term = termCounter.incrementAndGet();
          this.termStartTime = getWallClock().getTime().unixTimestamp();
        } else {
          this.registrations = updatedRegistrations;
          this.leader = null;
        }
      } else {
        this.registrations = updatedRegistrations;
      }
    }
  }

  protected void cleanup(PrimitiveSession session) {
    Optional<Registration> registration =
        registrations.stream().filter(r -> r.sessionId() == session.sessionId().id()).findFirst();
    if (registration.isPresent()) {
      List<Registration> updatedRegistrations =
          registrations.stream()
              .filter(r -> r.sessionId() != session.sessionId().id())
              .collect(Collectors.toList());
      if (leader.sessionId() == session.sessionId().id()) {
        if (!updatedRegistrations.isEmpty()) {
          this.registrations = updatedRegistrations;
          this.leader = updatedRegistrations.get(0);
          this.term = termCounter.incrementAndGet();
          this.termStartTime = getWallClock().getTime().unixTimestamp();
        } else {
          this.registrations = updatedRegistrations;
          this.leader = null;
        }
      } else {
        this.registrations = updatedRegistrations;
      }
    }
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

  protected void addRegistration(Registration registration) {
    if (registrations.stream().noneMatch(r -> Arrays.equals(registration.id(), r.id()))) {
      List<Registration> updatedRegistrations = new LinkedList<>(registrations);
      updatedRegistrations.add(registration);
      boolean newLeader = leader == null;
      this.registrations = updatedRegistrations;
      if (newLeader) {
        this.leader = registration;
        this.term = termCounter.incrementAndGet();
        this.termStartTime = getWallClock().getTime().unixTimestamp();
      }
    }
  }

  @Override
  public void onExpire(PrimitiveSession session) {
    onSessionEnd(session);
  }

  @Override
  public void onClose(PrimitiveSession session) {
    onSessionEnd(session);
  }
}