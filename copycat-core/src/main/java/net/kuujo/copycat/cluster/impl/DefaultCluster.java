package net.kuujo.copycat.cluster.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;

public class DefaultCluster implements Cluster, Observer {
  private final CopyCatContext context;
  private final DynamicClusterConfig config = new DynamicClusterConfig();
  private final Map<String, Member> members = new HashMap<>();

  public DefaultCluster(ClusterConfig config, CopyCatContext context) {
    this.context = context;
    this.config.addObserver(this);
    this.config.setLocalMember(config.getLocalMember());
    this.config.setRemoteMembers(config.getRemoteMembers());
  }

  @Override
  public void update(Observable o, Object arg) {
    clusterChanged((ClusterConfig) o);
  }

  /**
   * Called when the cluster configuration has changed.
   */
  private void clusterChanged(ClusterConfig config) {
    for (String member : config.getMembers()) {
      if (!members.containsKey(member)) {
        members.put(member, new DefaultMember(member, context));
      }
    }
    Iterator<Map.Entry<String, Member>> iterator = members.entrySet().iterator();
    while (iterator.hasNext()) {
      if (!config.getMembers().contains(iterator.next().getKey())) {
        iterator.remove();
      }
    }
  }

  @Override
  public CopyCatContext context() {
    return context;
  }

  @Override
  public ClusterConfig config() {
    return config;
  }

  @Override
  public Member localMember() {
    return members.get(config.getLocalMember());
  }

  @Override
  public Set<Member> members() {
    return new HashSet<Member>(members.values());
  }

  @Override
  public Member member(String address) {
    return members.get(address);
  }

}
