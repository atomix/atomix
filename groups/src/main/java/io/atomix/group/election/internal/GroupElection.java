/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.election.internal;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.Listeners;
import io.atomix.group.GroupMember;
import io.atomix.group.election.Election;
import io.atomix.group.election.Term;
import io.atomix.group.internal.MembershipGroup;

import java.util.function.Consumer;

/**
 * Group election client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupElection implements Election {
  private final MembershipGroup group;
  private final Listeners<Term> electionListeners = new Listeners<>();
  private volatile GroupTerm term = new GroupTerm(0);

  public GroupElection(MembershipGroup group) {
    this.group = Assert.notNull(group, "group");
  }

  @Override
  public Term term() {
    return term;
  }

  @Override
  public synchronized Listener<Term> onElection(Consumer<Term> callback) {
    Listener<Term> listener = electionListeners.add(callback);
    if (term != null && term.leader() != null) {
      listener.accept(term);
    }
    return listener;
  }

  /**
   * Sets the group term.
   */
  public synchronized void setTerm(long term) {
    onTerm(term);
  }

  /**
   * Called when the term changes.
   */
  public synchronized void onTerm(long term) {
    this.term = new GroupTerm(term);
  }

  /**
   * Sets the group leader.
   */
  public synchronized void setLeader(GroupMember leader) {
    term.setLeader(leader);
  }

  /**
   * Called when a member is elected.
   */
  public synchronized void onElection(GroupMember leader) {
    term.setLeader(leader);
    electionListeners.accept(term);
  }

  /**
   * Called when a leader resigns.
   */
  public void onResign() {
    term.setLeader(null);
  }

}
