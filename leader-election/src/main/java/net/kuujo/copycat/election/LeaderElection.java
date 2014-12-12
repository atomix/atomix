package net.kuujo.copycat.election;

import net.kuujo.copycat.CopycatCoordinator;
import net.kuujo.copycat.CopycatResource;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.election.internal.DefaultLeaderElection;
import net.kuujo.copycat.internal.DefaultCopycatCoordinator;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.log.LogConfig;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;

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
    return create(name, Services.load("copycat.cluster"), Services.load("copycat.protocol"), Services.load("copycat.log", LogConfig.class));
  }

  /**
   * Creates a new state machine for the given state model.
   *
   * @param name The election name.
   * @param config The Copycat cluster.
   * @param protocol The Copycat cluster protocol.
   * @param log The Copycat log configuration.
   * @return The state machine.
   */
  static LeaderElection create(String name, ClusterConfig config, Protocol protocol, LogConfig log) {
    CopycatCoordinator coordinator = new DefaultCopycatCoordinator(config, protocol, new InMemoryLog("coordinator", new LogConfig()), ExecutionContext.create());
    try {
      coordinator.open().get();
      DefaultLeaderElection election = new DefaultLeaderElection(name, coordinator);
      election.withShutdownTask(coordinator::close);
      return election;
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
