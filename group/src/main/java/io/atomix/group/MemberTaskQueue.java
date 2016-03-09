package io.atomix.group;

import io.atomix.catalyst.util.Assert;
import io.atomix.group.state.GroupCommands;

import java.util.concurrent.CompletableFuture;

/**
 * Member task queue.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class MemberTaskQueue extends GroupTaskQueue {
  private final String memberId;

  public MemberTaskQueue(MembershipGroup group, String memberId) {
    super(group);
    this.memberId = Assert.notNull(memberId, "memberId");
  }

  @Override
  protected CompletableFuture<Void> submit(long taskId, Object task) {
    return group.submit(new GroupCommands.Submit(memberId, taskId, task));
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s]", getClass().getSimpleName(), memberId);
  }

}
