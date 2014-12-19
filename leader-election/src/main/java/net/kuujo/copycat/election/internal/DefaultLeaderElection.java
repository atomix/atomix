package net.kuujo.copycat.election.internal;

import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.election.ElectionResult;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.internal.AbstractCopycatResource;
import net.kuujo.copycat.log.ZeroRetentionPolicy;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultLeaderElection extends AbstractCopycatResource<LeaderElection> implements LeaderElection {
  private Consumer<Member> handler;
  private final Consumer<ElectionResult> electionHandler = result -> {
    if (handler != null) {
      handler.accept(result.winner());
    }
  };

  public DefaultLeaderElection(String name, CopycatContext context, ClusterCoordinator coordinator, ExecutionContext executor) {
    super(name, context, coordinator, executor);
    context.log().config()
      .withFlushOnWrite(true)
      .withRetentionPolicy(new ZeroRetentionPolicy());
  }

  @Override
  public LeaderElection handler(Consumer<Member> handler) {
    this.handler = handler;
    return this;
  }

  @Override
  public CompletableFuture<Void> open() {
    return super.open().thenAccept(result -> {
      context.cluster().election().handler(electionHandler);
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    context.cluster().election().handler(null);
    return super.close();
  }

}
