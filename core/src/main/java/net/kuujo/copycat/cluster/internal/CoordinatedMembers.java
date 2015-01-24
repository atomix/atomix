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
package net.kuujo.copycat.cluster.internal;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.Members;
import net.kuujo.copycat.cluster.MembershipEvent;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Coordinates members implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedMembers implements Members {
  private final AbstractCluster cluster;
  final Map<String, CoordinatedMember> members;

  public CoordinatedMembers(Map<String, CoordinatedMember> members, AbstractCluster cluster) {
    this.members = members;
    this.cluster = cluster;
  }

  @Override
  public int size() {
    return members.size();
  }

  @Override
  public boolean isEmpty() {
    return members.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return members.values().contains(o);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Iterator iterator() {
    return members.values().iterator();
  }

  @Override
  public Object[] toArray() {
    return members.values().toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return members.values().toArray(a);
  }

  @Override
  public boolean add(Member member) {
    throw new UnsupportedOperationException("Cannot modify cluster membership");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("Cannot modify cluster membership");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return members.values().containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends Member> c) {
    throw new UnsupportedOperationException("Cannot modify cluster membership");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("Cannot modify cluster membership");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Cannot modify cluster membership");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Cannot modify cluster membership");
  }

  @Override
  public Members addListener(EventListener<MembershipEvent> listener) {
    cluster.addMembershipListener(listener);
    return this;
  }

}
