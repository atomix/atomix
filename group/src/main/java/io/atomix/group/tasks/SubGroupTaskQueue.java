package io.atomix.group.tasks;

import io.atomix.catalyst.util.Assert;
import io.atomix.group.GroupMember;
import io.atomix.group.MembershipGroup;
import io.atomix.group.SubGroup;

import java.util.Collection;

/**
 * Membership group task queue.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SubGroupTaskQueue extends GroupTaskQueue {
  private final SubGroup subGroup;

  public SubGroupTaskQueue(SubGroup subGroup, MembershipGroup group) {
    super(group);
    this.subGroup = Assert.notNull(subGroup, "subGroup");
  }

  @Override
  protected Collection<GroupMember> members() {
    return subGroup.members();
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getSimpleName(), subGroup.members());
  }

}
