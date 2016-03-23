package io.atomix.group.tasks;

import io.atomix.catalyst.util.Assert;
import io.atomix.group.MembershipGroup;
import io.atomix.group.state.GroupCommands;
import io.atomix.group.util.Submitter;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Member task queue.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MemberTaskQueue extends TaskQueue {
  private final String memberId;
  private final Submitter submitter;
  protected long taskId;
  protected final Map<Long, CompletableFuture<Void>> taskFutures = new ConcurrentHashMap<>();

  public MemberTaskQueue(String memberId, MembershipGroup group, Submitter submitter) {
    super(group);
    this.memberId = Assert.notNull(memberId, "memberId");
    this.submitter = Assert.notNull(submitter, "submitter");
  }

  @Override
  public CompletableFuture<Void> submit(Object task) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    final long taskId = ++this.taskId;
    taskFutures.put(taskId, future);
    submitter.submit(new GroupCommands.Submit(memberId, taskId, task)).whenComplete((result, error) -> {
      if (error != null) {
        taskFutures.remove(taskId);
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Handles a task acknowledgement.
   */
  void onAck(long taskId) {
    CompletableFuture<Void> future = taskFutures.remove(taskId);
    if (future != null) {
      future.complete(null);
    }
  }

  /**
   * Handles a task failure.
   */
  void onFail(long taskId) {
    CompletableFuture<Void> future = taskFutures.remove(taskId);
    if (future != null) {
      future.completeExceptionally(new TaskFailedException());
    }
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s]", getClass().getSimpleName(), memberId);
  }

}
