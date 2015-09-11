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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.io.transport.Address;
import net.kuujo.copycat.util.Assert;

import java.util.*;

/**
 * Cluster state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ClusterState {
  private final ServerContext context;
  private final Address address;
  private Type type = Type.PASSIVE;
  private long version = -1;
  private final Map<Integer, MemberState> membersMap = new HashMap<>();
  private final Map<Integer, Type> types = new HashMap<>();
  private final List<MemberState> members = new ArrayList<>();
  private final List<MemberState> activeMembers = new ArrayList<>();
  private final List<MemberState> passiveMembers = new ArrayList<>();

  /**
   * Member state type.
   */
  private enum Type {
    ACTIVE,
    PASSIVE
  }

  ClusterState(ServerContext context, Address address) {
    this.context = Assert.notNull(context, "context");
    this.address = Assert.notNull(address, "address");
  }

  /**
   * Returns the local cluster member address.
   *
   * @return The local cluster member address.
   */
  Address getAddress() {
    return address;
  }

  /**
   * Returns a boolean value indicating whether the local member is active.
   *
   * @return Indicates whether the local member is active.
   */
  boolean isActive() {
    return type == Type.ACTIVE;
  }

  /**
   * Sets whether the local member is active.
   *
   * @param active Whether the local member is active.
   * @return The cluster state.
   */
  ClusterState setActive(boolean active) {
    type = active ? Type.ACTIVE : Type.PASSIVE;
    return this;
  }

  /**
   * Returns a boolean value indicating whether the local member is passive.
   *
   * @return Indicates whether the local member is passive.
   */
  boolean isPassive() {
    return type == Type.PASSIVE;
  }

  /**
   * Sets whether the local member is passive.
   *
   * @param passive Whether the local member is passive.
   * @return The cluster state.
   */
  ClusterState setPassive(boolean passive) {
    type = passive ? Type.PASSIVE : Type.ACTIVE;
    return this;
  }

  /**
   * Returns the remote quorum count.
   *
   * @return The remote quorum count.
   */
  int getQuorum() {
    return (int) Math.ceil(activeMembers.size() / 2.0);
  }

  /**
   * Returns the cluster state version.
   *
   * @return The cluster state version.
   */
  long getVersion() {
    return version;
  }

  /**
   * Clears all members from the cluster state.
   *
   * @return The cluster state.
   */
  private ClusterState clearMembers() {
    members.clear();
    activeMembers.clear();
    passiveMembers.clear();
    membersMap.clear();
    types.clear();
    return this;
  }

  /**
   * Returns a member by ID.
   *
   * @param id The member ID.
   * @return The member state.
   */
  MemberState getMember(int id) {
    return membersMap.get(id);
  }

  /**
   * Returns a boolean value indicating whether the given member is active.
   *
   * @param member The member state.
   * @return Indicates whether the member is active.
   */
  boolean isActiveMember(MemberState member) {
    return types.get(member.getAddress().hashCode()) == Type.ACTIVE;
  }

  /**
   * Returns a boolean value indicating whether the given member is passive.
   *
   * @param member The member state.
   * @return Indicates whether the member is passive.
   */
  boolean isPassiveMember(MemberState member) {
    return types.get(member.getAddress().hashCode()) == Type.PASSIVE;
  }

  /**
   * Returns a list of passive members.
   *
   * @return A list of passive members.
   */
  List<MemberState> getPassiveMembers() {
    return passiveMembers;
  }

  /**
   * Returns a list of passive members.
   *
   * @param comparator A comparator with which to sort the members list.
   * @return The sorted members list.
   */
  List<MemberState> getPassiveMembers(Comparator<MemberState> comparator) {
    Collections.sort(passiveMembers, comparator);
    return passiveMembers;
  }

  /**
   * Returns a list of active members.
   *
   * @return A list of active members.
   */
  List<MemberState> getActiveMembers() {
    return activeMembers;
  }

  /**
   * Returns a list of active members.
   *
   * @param comparator A comparator with which to sort the members list.
   * @return The sorted members list.
   */
  List<MemberState> getActiveMembers(Comparator<MemberState> comparator) {
    Collections.sort(activeMembers, comparator);
    return activeMembers;
  }

  /**
   * Returns a list of all members.
   *
   * @return A list of all members.
   */
  List<MemberState> getMembers() {
    return members;
  }

  /**
   * Configures the cluster state.
   *
   * @param version The cluster state version.
   * @param activeMembers The active members.
   * @param passiveMembers The passive members.
   * @return The cluster state.
   */
  ClusterState configure(long version, Collection<Address> activeMembers, Collection<Address> passiveMembers) {
    if (version <= this.version)
      return this;

    List<MemberState> newActiveMembers = buildMembers(activeMembers);
    List<MemberState> newPassiveMembers = buildMembers(passiveMembers);

    clearMembers();

    for (MemberState member : newActiveMembers) {
      membersMap.put(member.getAddress().hashCode(), member);
      members.add(member);
      this.activeMembers.add(member);
      types.put(member.getAddress().hashCode(), Type.ACTIVE);
    }

    for (MemberState member : newPassiveMembers) {
      membersMap.put(member.getAddress().hashCode(), member);
      members.add(member);
      this.passiveMembers.add(member);
      types.put(member.getAddress().hashCode(), Type.PASSIVE);
    }

    if (activeMembers.contains(address)) {
      type = Type.ACTIVE;
    } else if (passiveMembers.contains(address)) {
      type = Type.PASSIVE;
    } else {
      type = null;
    }

    this.version = version;

    return this;
  }

  /**
   * Builds a list of active members.
   */
  Collection<Address> buildActiveMembers() {
    List<Address> members = new ArrayList<>();
    for (MemberState state : activeMembers) {
      members.add(state.getAddress());
    }
    members.add(address);
    return members;
  }

  /**
   * Builds a list of passive members.
   */
  Collection<Address> buildPassiveMembers() {
    List<Address> members = new ArrayList<>();
    for (MemberState state : passiveMembers) {
      members.add(state.getAddress());
    }
    return members;
  }

  /**
   * Builds a members list.
   */
  private List<MemberState> buildMembers(Collection<Address> members) {
    List<MemberState> states = new ArrayList<>(members.size());
    for (Address address : members) {
      if (!address.equals(this.address)) {
        MemberState state = membersMap.get(address.hashCode());
        if (state == null) {
          state = new MemberState(address);
          state.setNextIndex(Math.max(state.getMatchIndex(), Math.max(context.getLog().lastIndex(), 1)));
        }
        states.add(state);
      }
    }
    return states;
  }

}
