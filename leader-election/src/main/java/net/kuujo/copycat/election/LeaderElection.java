package net.kuujo.copycat.election;

import net.kuujo.copycat.Coordinator;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.election.internal.DefaultLeaderElection;
import net.kuujo.copycat.internal.DefaultCoordinator;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LeaderElection extends Resource {

  /**
   * Creates a new leader election for the given state model.
   *
   * @param name The election name.
   * @return The state machine.
   */
  static LeaderElection create(String name) {
    return create(name, Services.load("copycat.cluster"), Services.load("copycat.protocol"));
  }

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The election name.
   * @param config The Copycat cluster.
   * @param protocol The Copycat cluster protocol.
   * @return The state machine.
   */
  static LeaderElection create(String name, ClusterConfig config, Protocol protocol) {
    ExecutionContext executor = ExecutionContext.create();
    Coordinator coordinator = new DefaultCoordinator(config, protocol, new InMemoryLog(), executor);
    try {
      return coordinator.<LeaderElection>createResource(name, resource -> new InMemoryLog(), (resource, coord, cluster, context) -> {
        return (LeaderElection) new DefaultLeaderElection(resource, coord, cluster, context).withShutdownTask(coordinator::close);
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Registers a leader election handler.
   *
   * @param handler The leader election handler.
   * @return The leader election.
   */
  LeaderElection handler(Consumer<Member> handler);

}
