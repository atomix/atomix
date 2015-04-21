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

import net.kuujo.copycat.cluster.protocol.Client;
import net.kuujo.copycat.cluster.protocol.ClientFactory;
import net.kuujo.copycat.io.serializer.CopycatSerializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Copycat cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Cluster {

  /**
   * Returns a new cluster builder.
   *
   * @return A new cluster builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  protected final LocalMember localMember;
  protected final Map<Integer, RemoteMember> remoteMembers = new ConcurrentHashMap<>();
  protected final Map<Integer, Member> members = new ConcurrentHashMap<>();
  protected final ClientFactory clientFactory;
  protected final CopycatSerializer serializer = new CopycatSerializer();
  protected final Set<EventListener<MembershipEvent>> membershipListeners = new CopyOnWriteArraySet<>();
  private final AtomicInteger permits = new AtomicInteger();
  private MembershipDetector membershipDetector;
  private CompletableFuture<Cluster> openFuture;
  private CompletableFuture<Void> closeFuture;

  private Cluster(LocalMember localMember, Collection<RemoteMember> remoteMembers, ClientFactory clientFactory) {
    this.localMember = localMember;
    remoteMembers.forEach(m -> this.remoteMembers.put(m.id(), m));
    this.members.putAll(this.remoteMembers);
    this.members.put(localMember.id(), localMember);
    this.clientFactory = clientFactory;
  }

  /**
   * Creates a new client.
   */
  protected Client createClient(String address) {
    return clientFactory.createClient(address);
  }

  /**
   * Returns the local cluster member.<p>
   *
   * When messages are sent to the local member, Copycat will send the messages asynchronously on an event loop.
   *
   * @return The local cluster member.
   */
  public LocalMember member() {
    return localMember;
  }

  /**
   * Returns a member by member identifier.
   *
   * @param id The unique member identifier.
   * @return The member or {@code null} if the member does not exist.
   */
  public Member member(int id) {
    if (localMember.id() == id) {
      return localMember;
    }
    Member member = remoteMembers.get(id);
    if (member == null)
      throw new NoSuchElementException();
    return member;
  }

  /**
   * Returns a member by member address.
   *
   * @param address The unique member address.
   * @return The member or {@code null} if the member does not exist.
   * @throws NullPointerException If the given {@code address} is {@code null}
   */
  public Member member(String address) {
    if (localMember.address().equals(address)) {
      return localMember;
    }
    return remoteMembers.values().stream().filter(m -> m.address().equals(address)).findFirst().get();
  }

  /**
   * Returns an immutable set of all cluster members.
   *
   * @return An immutable set of all members in the cluster.
   */
  public Collection<Member> members() {
    return members.values();
  }

  /**
   * Broadcasts a message to the cluster.<p>
   *
   * Message broadcasting to the Copycat cluster should not be considered reliable. Copycat sends broadcast messages to
   * all currently known members of the cluster in a fire-and-forget manner. Broadcast messages do not support replies.
   * To send messages with replies to all members of the cluster, iterate over the collection of
   * {@link net.kuujo.copycat.cluster.Cluster#members()} in the cluster.
   *
   * @param topic The topic to which to broadcast the message. Members with broadcast listeners registered for this
   *              topic will receive the broadcast message.
   * @param message The message to broadcast. This will be serialized using the serializer configured in the resource
   *                configuration.
   * @param <T> The message type.
   * @return The cluster.
   */
  public <T> Cluster broadcast(String topic, T message) {
    if (!isOpen())
      throw new IllegalStateException("cluster not open");
    remoteMembers.values().forEach(m -> {
      m.send(topic, message);
    });
    return this;
  }

  /**
   * Adds a membership listener to the cluster.<p>
   *
   * Membership listeners are triggered when {@link Member.Type#PASSIVE} members join or leave the cluster. Copycat uses
   * a gossip based failure detection algorithm to detect failures, using vector clocks to version cluster
   * configurations. In order to prevent false positives due to network partitions, Copycat's failure detection
   * algorithm will attempt to contact a member from up to three different nodes before considering that node failed.
   * If the membership listener is called with a {@link MembershipEvent.Type#LEAVE} event, that indicates that Copycat
   * has attempted to contact the missing member multiple times.<p>
   *
   * {@link Member.Type#ACTIVE} members never join or leave the cluster since they are explicitly configured, active,
   * voting members of the cluster. However, this may change at some point in the future to allow failure detection for
   * active members as well.
   *
   * @param listener The membership event listener to add.
   * @return The cluster.
   */
  public Cluster addMembershipListener(EventListener<MembershipEvent> listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    membershipListeners.add(listener);
    return this;
  }

  /**
   * Removes a membership listener from the cluster.
   *
   * @param listener The membership event listener to remove.
   * @return The cluster.
   */
  public Cluster removeMembershipListener(EventListener<MembershipEvent> listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    membershipListeners.remove(listener);
    return this;
  }

  /**
   * Opens the cluster.
   *
   * @return A completable future to be completed once the cluster has been opened.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Cluster> open() {
    if (permits.incrementAndGet() == 1) {
      synchronized (this) {
        if (openFuture == null) {
          openFuture = localMember.listen().thenCompose(v -> {
            int i = 0;
            CompletableFuture<? extends Member>[] futures = new CompletableFuture[members.size() - 1];
            for (RemoteMember member : remoteMembers.values()) {
              futures[i++] = member.connect();
            }
            return CompletableFuture.allOf(futures);
          }).thenApply(v -> {
            membershipDetector = new MembershipDetector(this);
            return this;
          });
        }
      }
      return openFuture;
    }
    return CompletableFuture.completedFuture(this);
  }

  /**
   * Returns a boolean value indicating whether the cluster is open.
   *
   * @return Indicates whether the cluster is open.
   */
  public boolean isOpen() {
    return permits.get() > 0 && openFuture == null;
  }

  /**
   * Closes the cluster.
   *
   * @return A completable future to be completed once the cluster has been closed.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    if (permits.decrementAndGet() == 0) {
      synchronized (this) {
        if (closeFuture == null) {
          int i = 0;
          CompletableFuture<? extends Member>[] futures = new CompletableFuture[members.size()-1];
          for (RemoteMember member : remoteMembers.values()) {
            futures[i++] = member.connect();
          }
          closeFuture = CompletableFuture.allOf(futures)
            .thenCompose(v -> localMember.close())
            .thenRun(() -> {
              if (membershipDetector != null) {
                membershipDetector.close();
                membershipDetector = null;
              }
            });
        }
      }
    }
    return closeFuture;
  }

  /**
   * Returns a boolean value indicating whether the cluster is closed.
   *
   * @return Indicates whether the cluster is closed.
   */
  public boolean isClosed() {
    return permits.get() == 0 && closeFuture == null;
  }

  /**
   * Cluster builder.
   */
  public static class Builder {
    private ClientFactory clientFactory;
    private LocalMember localMember;
    private Collection<RemoteMember> remoteMembers = new HashSet<>();

    private Builder() {
    }

    /**
     * Sets the cluster client factory.
     *
     * @param factory The cluster client factory.
     * @return The cluster builder.
     */
    public Builder withClientFactory(ClientFactory factory) {
      if (factory == null)
        throw new NullPointerException("factory cannot be null");
      this.clientFactory = factory;
      return this;
    }

    /**
     * Sets the local cluster member.
     *
     * @param member The local cluster member.
     * @return The cluster builder.
     */
    public Builder withLocalMember(LocalMember member) {
      if (member == null)
        throw new NullPointerException("member cannot be null");
      localMember = member;
      return this;
    }

    /**
     * Sets the set of remote cluster members.
     *
     * @param members The set of remote cluster members.
     * @return The cluster builder.
     */
    public Builder withRemoteMembers(RemoteMember... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return withRemoteMembers(Arrays.asList(members));
    }

    /**
     * Sets the set of remote cluster members.
     *
     * @param members The set of remote cluster members.
     * @return The cluster builder.
     */
    public Builder withRemoteMembers(Collection<RemoteMember> members) {
      remoteMembers.clear();
      remoteMembers.addAll(members);
      return this;
    }

    /**
     * Adds a remote member to the cluster.
     *
     * @param member The remote member to add.
     * @return The cluster builder.
     */
    public Builder addRemoteMember(RemoteMember member) {
      return addRemoteMembers(Collections.singleton(member));
    }

    /**
     * Adds a set of remote members to the cluster.
     *
     * @param members The set of remote members to add.
     * @return The cluster builder.
     */
    public Builder addRemoteMembers(RemoteMember... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return addRemoteMembers(Arrays.asList(members));
    }

    /**
     * Adds a set of remote members to the cluster.
     *
     * @param members The set of remote members to add.
     * @return The cluster builder.
     */
    public Builder addRemoteMembers(Collection<RemoteMember> members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      remoteMembers.addAll(members);
      return this;
    }

    /**
     * Sets the set of cluster members.
     *
     * @param members The set of cluster members.
     * @return The cluster builder.
     */
    public Builder withMembers(Member... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return withMembers(Arrays.asList(members));
    }

    /**
     * Sets the set of cluster members.
     *
     * @param members The set of cluster members.
     * @return The cluster builder.
     */
    public Builder withMembers(Collection<Member> members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      remoteMembers.clear();
      return addMembers(members);
    }

    /**
     * Adds a member to the cluster.
     *
     * @param member The member to add.
     * @return The cluster builder.
     */
    public Builder addMember(Member member) {
      if (member == null)
        throw new NullPointerException("member cannot be null");
      return addMembers(Collections.singleton(member));
    }

    /**
     * Adds a set of members to the cluster.
     *
     * @param members The set of members to add.
     * @return The cluster builder.
     */
    public Builder addMembers(Member... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return addMembers(Arrays.asList(members));
    }

    /**
     * Adds a set of members to the cluster.
     *
     * @param members The set of members to add.
     * @return The cluster builder.
     */
    public Builder addMembers(Collection<Member> members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      members.forEach(m -> {
        if (m instanceof LocalMember) {
          withLocalMember((LocalMember) m);
        } else if (m instanceof RemoteMember) {
          addRemoteMember((RemoteMember) m);
        }
      });
      return this;
    }

    /**
     * Builds the cluster.
     *
     * @return The cluster.
     */
    public Cluster build() {
      if (clientFactory == null)
        throw new NullPointerException("client factory cannot be null");
      if (localMember == null)
        throw new NullPointerException("local member cannot be null");
      return new Cluster(localMember, remoteMembers, clientFactory);
    }
  }

}
