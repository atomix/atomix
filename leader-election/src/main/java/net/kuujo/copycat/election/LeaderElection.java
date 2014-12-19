package net.kuujo.copycat.election;

import net.kuujo.copycat.CopycatResource;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.election.internal.DefaultLeaderElection;
import net.kuujo.copycat.internal.cluster.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LeaderElection extends CopycatResource {

  /**
   * Creates a new leader election for the given state model.
   *
   * @param name The election name.
   * @return The state machine.
   */
  static LeaderElection create(String name) {
    return create(name, new ClusterConfig(), ExecutionContext.create());
  }

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The election name.
   * @param context The user execution context.
   * @return The state machine.
   */
  static LeaderElection create(String name, ExecutionContext context) {
    return create(name, new ClusterConfig(), context);
  }

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The election name.
   * @param cluster The Copycat cluster.
   * @return The state machine.
   */
  static LeaderElection create(String name, ClusterConfig cluster) {
    return create(name, cluster, ExecutionContext.create());
  }

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The election name.
   * @param cluster The Copycat cluster.
   * @param context The user execution context.
   * @return The state machine.
   */
  static LeaderElection create(String name, ClusterConfig cluster, ExecutionContext context) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(cluster, ExecutionContext.create());
    try {
      coordinator.open().get();
      return new DefaultLeaderElection(name, coordinator.createResource(name).get(), coordinator, context);
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
