/*
 * Copyright 2014 the original author or authors.
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

/**
 * Default cluster implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCluster implements Cluster, Observer {
  private final CopyCatContext context;
  private final DynamicClusterConfig config = new DynamicClusterConfig();
  private final Map<String, Member> members = new HashMap<>();

  public DefaultCluster(ClusterConfig config, CopyCatContext context) {
    this.context = context;
    this.config.addObserver(this);
    this.config.setLocalMember(config.getLocalMember());
    this.config.setRemoteMembers(config.getRemoteMembers());
    clusterChanged(this.config);
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
