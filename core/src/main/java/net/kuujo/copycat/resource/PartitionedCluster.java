package net.kuujo.copycat.resource;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.*;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Partitioned resource cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class PartitionedCluster implements Cluster {
  private final Cluster cluster;
  private final ReplicationStrategy replicationStrategy;
  private final int partitionId;
  private final int partitions;
  private final Map<EventListener<MembershipChangeEvent>, EventListener<MembershipChangeEvent>> listeners = new ConcurrentHashMap<>();
  private LocalMember localMember;
  private Map<Integer, Member> members = new ConcurrentHashMap<>();
  private Map<Integer, Member> remoteMembers = new ConcurrentHashMap<>();

  PartitionedCluster(Cluster cluster, ReplicationStrategy replicationStrategy, int partitionId, int partitions) {
    this.cluster = cluster;
    this.replicationStrategy = replicationStrategy;
    this.partitionId = partitionId;
    this.partitions = partitions;
    cluster.addMembershipListener(event -> {
      resetMembers();
    });
    resetMembers();
  }

  /**
   * Resets the partitioned cluster membership.
   */
  private void resetMembers() {
    LocalMember localMember = null;
    Map<Integer, Member> members = new ConcurrentHashMap<>();
    for (Member member : replicationStrategy.selectPrimaries(cluster, partitionId, partitions)) {
      if (member instanceof LocalMember) {
        localMember = new PartitionedLocalMember((LocalMember) member, Member.Type.ACTIVE);
      } else if (member.type() != Member.Type.REMOTE) {
        members.put(member.id(), member);
      }
    }

    for (Member member : replicationStrategy.selectSecondaries(cluster, partitionId, partitions)) {
      if (member instanceof LocalMember) {
        localMember = new PartitionedLocalMember((LocalMember) member, Member.Type.PASSIVE);
      } else if (member.type() != Member.Type.REMOTE) {
        members.put(member.id(), new PartitionedRemoteMember(member, Member.Type.PASSIVE));
      }
    }

    for (Member member : cluster.members()) {
      if (member instanceof RemoteMember && member.type() == Member.Type.REMOTE) {
        members.put(member.id(), member);
      }
    }

    this.remoteMembers = members;
    this.members = new ConcurrentHashMap<>(members);
    if (localMember != null) {
      this.localMember = localMember;
      this.members.put(localMember.id(), localMember);
    } else {
      this.localMember = cluster.member();
      this.members.put(this.localMember.id(), this.localMember);
    }
  }

  @Override
  public Cluster addMembershipListener(EventListener<MembershipChangeEvent> listener) {
    EventListener<MembershipChangeEvent> wrappedListener = event -> {
      if (members.containsKey(event.member().id())) {
        listener.accept(event);
      }
    };
    listeners.put(listener, wrappedListener);
    cluster.addMembershipListener(wrappedListener);
    return this;
  }

  @Override
  public Cluster removeMembershipListener(EventListener<MembershipChangeEvent> listener) {
    EventListener<MembershipChangeEvent> wrappedListener = listeners.remove(listener);
    if (wrappedListener != null) {
      cluster.removeMembershipListener(wrappedListener);
    }
    return this;
  }

  @Override
  public LocalMember member() {
    return localMember;
  }

  @Override
  public Member member(int id) {
    if (localMember.id() == id)
      return localMember;
    return remoteMembers.get(id);
  }

  @Override
  public Collection<Member> members() {
    return members.values();
  }

  @Override
  public <T> Cluster broadcast(String topic, T message) {
    remoteMembers.values().forEach(m -> {
      m.send(topic, message);
    });
    return this;
  }

  @Override
  public String toString() {
    return String.format("%s[%s]", getClass().getSimpleName(), members.values());
  }

  /**
   * Partitioned local member.
   */
  private static class PartitionedLocalMember implements LocalMember {
    private final LocalMember member;
    private final Member.Type type;

    private PartitionedLocalMember(LocalMember member, Member.Type type) {
      this.member = member;
      this.type = type;
    }

    @Override
    public <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler) {
      member.registerHandler(topic, handler);
      return this;
    }

    @Override
    public LocalMember unregisterHandler(String topic) {
      member.unregisterHandler(topic);
      return this;
    }

    @Override
    public int id() {
      return member.id();
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public Status status() {
      return member.status();
    }

    @Override
    public <T, U> CompletableFuture<U> send(String topic, T message) {
      return member.send(topic, message);
    }

    @Override
    public CompletableFuture<Void> execute(Task<Void> task) {
      return member.execute(task);
    }

    @Override
    public <T> CompletableFuture<T> submit(Task<T> task) {
      return member.submit(task);
    }

    @Override
    public String toString() {
      return String.format("%s[id=%s, type=%s, status=%s]", getClass().getSimpleName(), member.id(), member.type(), member.status());
    }
  }

  /**
   * Partitioned remote member.
   */
  private static class PartitionedRemoteMember implements RemoteMember {
    private final Member member;
    private final Member.Type type;

    private PartitionedRemoteMember(Member member, Member.Type type) {
      this.member = member;
      this.type = type;
    }

    @Override
    public int id() {
      return member.id();
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public Status status() {
      return member.status();
    }

    @Override
    public <T, U> CompletableFuture<U> send(String topic, T message) {
      return member.send(topic, message);
    }

    @Override
    public CompletableFuture<Void> execute(Task<Void> task) {
      return member.execute(task);
    }

    @Override
    public <T> CompletableFuture<T> submit(Task<T> task) {
      return member.submit(task);
    }

    @Override
    public String toString() {
      return String.format("%s[id=%s, type=%s, status=%s]", getClass().getSimpleName(), member.id(), member.type(), member.status());
    }
  }

}
