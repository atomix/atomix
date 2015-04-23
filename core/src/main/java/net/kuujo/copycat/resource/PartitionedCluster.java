package net.kuujo.copycat.resource;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.MembershipChangeEvent;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Partitioned resource cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class PartitionedCluster implements Cluster {
  private final Cluster cluster;
  private final ReplicationStrategy replicationStrategy;
  private final int partitions;
  private final Map<EventListener<MembershipChangeEvent>, EventListener<MembershipChangeEvent>> listeners = new ConcurrentHashMap<>();
  private final LocalMember localMember;
  private Map<Integer, Member> members = new ConcurrentHashMap<>();
  private Map<Integer, Member> remoteMembers = new ConcurrentHashMap<>();

  PartitionedCluster(Cluster cluster, ReplicationStrategy replicationStrategy, int partitions) {
    this.cluster = cluster;
    this.localMember = cluster.member();
    this.replicationStrategy = replicationStrategy;
    this.partitions = partitions;
    cluster.addMembershipListener(event -> {
      resetMembers();
    });
    resetMembers();
  }

  private void resetMembers() {
    Map<Integer, Member> members = new ConcurrentHashMap<>();
    replicationStrategy.selectPrimaries(cluster, partitions).forEach(member -> {
      if (!member.equals(localMember)) {
        members.put(member.id(), member);
      }
    });
    replicationStrategy.selectSecondaries(cluster, partitions).forEach(member -> {
      if (!members.equals(localMember)) {
        members.put(member.id(), member);
      }
    });
    this.remoteMembers = members;
    this.members = new ConcurrentHashMap<>(members);
    this.members.put(localMember.id(), localMember);
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

}
