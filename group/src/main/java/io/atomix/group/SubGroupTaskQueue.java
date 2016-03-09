package io.atomix.group;

import io.atomix.catalyst.util.Assert;
import io.atomix.group.state.GroupCommands;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Membership group task queue.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class SubGroupTaskQueue extends GroupTaskQueue {
  private final SubGroup subGroup;

  public SubGroupTaskQueue(MembershipGroup group, SubGroup subGroup) {
    super(group);
    this.subGroup = Assert.notNull(subGroup, "subGroup");
  }

  @Override
  protected CompletableFuture<Void> submit(long taskId, Object task) {
    synchronized (group) {
      Collection<GroupMember> members = subGroup.members();
      CompletableFuture[] futures = new CompletableFuture[members.size()];
      int i = 0;
      for (GroupMember member : members) {
        futures[i++] = group.submit(new GroupCommands.Submit(member.id(), taskId, task));
      }
      return CompletableFuture.allOf(futures);
    }
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getSimpleName(), subGroup.members());
  }

}
