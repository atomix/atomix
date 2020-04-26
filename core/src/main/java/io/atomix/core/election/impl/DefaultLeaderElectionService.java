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
import com.google.common.collect.Lists;
import io.atomix.core.election.Leader;
import io.atomix.core.election.LeaderElectionType;
import io.atomix.core.election.Leadership;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.misc.ArraySizeHashPrinter;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * State machine for {@link LeaderElectionProxy} resource.
 */
public class DefaultLeaderElectionService extends AbstractPrimitiveService<LeaderElectionClient> implements LeaderElectionService {

  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(LeaderElectionType.instance().namespace())
      .register(SessionId.class)
      .register(Registration.class)
      .register(new LinkedHashMap<>().keySet().getClass())
      .build());

  private Registration leader;
  private long term;
  private long termStartTime;
  private List<Registration> registrations = new LinkedList<>();
  private AtomicLong termCounter = new AtomicLong();
  private Set<SessionId> listeners = new LinkedHashSet<>();

  public DefaultLeaderElectionService() {
    super(LeaderElectionType.instance(), LeaderElectionClient.class);
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
    writer.writeObject(listeners);
    getLogger().debug("Took state machine snapshot");
  }

  @Override
  public void restore(BackupInput reader) {
    termCounter.set(reader.readLong());
    leader = reader.readObject();
    term = reader.readLong();
    termStartTime = reader.readLong();
    registrations = reader.readObject();
    listeners = reader.readObject();
    getLogger().debug("Reinstated state machine from snapshot");
  }

  @Override
  public void listen() {
    listeners.add(getCurrentSession().sessionId());
  }

  @Override
  public void unlisten() {
    listeners.remove(getCurrentSession().sessionId());
  }

  private void notifyLeadershipChange(Leadership<byte[]> oldLeadership, Leadership<byte[]> newLeadership) {
    listeners.forEach(id -> getSession(id).accept(client -> client.onLeadershipChange(oldLeadership, newLeadership)));
  }

  @Override
  public Leadership<byte[]> run(byte[] id) {
    try {
      Leadership<byte[]> oldLeadership = leadership();
      Registration registration = new Registration(id, getCurrentSession().sessionId().id());
      addRegistration(registration);
      Leadership<byte[]> newLeadership = leadership();

      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
      return newLeadership;
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void withdraw(byte[] id) {
    try {
      Leadership<byte[]> oldLeadership = leadership();
      cleanup(id);
      Leadership<byte[]> newLeadership = leadership();
      if (!Objects.equal(oldLeadership, newLeadership)) {
        notifyLeadershipChange(oldLeadership, newLeadership);
      }
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean anoint(byte[] id) {
    try {
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
      return leader != null && Arrays.equals(id, leader.id());
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean promote(byte[] id) {
    try {
      Leadership<byte[]> oldLeadership = leadership();
      boolean containsCandidate = oldLeadership.candidates().stream()
          .anyMatch(a -> Arrays.equals(a, id));
      if (!containsCandidate) {
        return false;
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
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void evict(byte[] id) {
    try {
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
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Leadership<byte[]> getLeadership() {
    try {
      return leadership();
    } catch (Exception e) {
      getLogger().error("State machine operation failed", e);
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  private Leadership<byte[]> leadership() {
    return new Leadership<>(leader(), candidates());
  }

  private void onSessionEnd(Session session) {
    listeners.remove(session.sessionId());
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

  protected void cleanup(Session session) {
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
  public void onExpire(Session session) {
    onSessionEnd(session);
  }

  @Override
  public void onClose(Session session) {
    onSessionEnd(session);
  }
}
