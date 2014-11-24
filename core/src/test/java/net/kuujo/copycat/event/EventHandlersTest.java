package net.kuujo.copycat.event;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.ClusterMember;
import net.kuujo.copycat.internal.event.DefaultEventHandlers;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Default event handlers test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class EventHandlersTest {
  private DefaultEventHandlers handlers;

  @BeforeMethod
  protected void beforeMethod() {
    handlers = new DefaultEventHandlers();
  }

  /**
   * Tests registering and unregistering a start handler by method.
   */
  public void testRegisterAndUnregisterStartHandlerByMethod() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<StartEvent> handler = (event) -> succeeded.set(true);
    handlers.start().registerHandler(handler);
    handlers.start().handle(new StartEvent());
    assertTrue(succeeded.get());
    handlers.start().unregisterHandler(handler);
    succeeded.set(false);
    handlers.start().handle(new StartEvent());
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a start handler by event.
   */
  public void testRegisterAndUnregisterStartHandlerByEvent() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<StartEvent> handler = (event) -> succeeded.set(true);
    handlers.event(StartEvent.class).registerHandler(handler);
    handlers.event(StartEvent.class).handle(new StartEvent());
    assertTrue(succeeded.get());
    handlers.event(StartEvent.class).unregisterHandler(handler);
    succeeded.set(false);
    handlers.event(StartEvent.class).handle(new StartEvent());
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a stop handler by method.
   */
  public void testRegisterAndUnregisterStopHandlerByMethod() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<StopEvent> handler = (event) -> succeeded.set(true);
    handlers.stop().registerHandler(handler);
    handlers.stop().handle(new StopEvent());
    assertTrue(succeeded.get());
    handlers.stop().unregisterHandler(handler);
    succeeded.set(false);
    handlers.stop().handle(new StopEvent());
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a stop handler by event.
   */
  public void testRegisterAndUnregisterStopHandlerByEvent() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<StopEvent> handler = (event) -> succeeded.set(true);
    handlers.event(StopEvent.class).registerHandler(handler);
    handlers.event(StopEvent.class).handle(new StopEvent());
    assertTrue(succeeded.get());
    handlers.event(StopEvent.class).unregisterHandler(handler);
    succeeded.set(false);
    handlers.event(StopEvent.class).handle(new StopEvent());
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a vote cast handler by method.
   */
  public void testRegisterAndUnregisterVoteCastHandlerByMethod() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<VoteCastEvent> handler = (event) -> succeeded.set(true);
    handlers.voteCast().registerHandler(handler);
    handlers.voteCast().handle(new VoteCastEvent(1, new ClusterMember("foo")));
    assertTrue(succeeded.get());
    handlers.voteCast().unregisterHandler(handler);
    succeeded.set(false);
    handlers.voteCast().handle(new VoteCastEvent(1, new ClusterMember("foo")));
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a vote cast handler by event.
   */
  public void testRegisterAndUnregisterVoteCastHandlerByEvent() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<VoteCastEvent> handler = (event) -> succeeded.set(true);
    handlers.event(VoteCastEvent.class).registerHandler(handler);
    handlers.event(VoteCastEvent.class).handle(new VoteCastEvent(1, new ClusterMember("foo")));
    assertTrue(succeeded.get());
    handlers.event(VoteCastEvent.class).unregisterHandler(handler);
    succeeded.set(false);
    handlers.event(VoteCastEvent.class).handle(new VoteCastEvent(1, new ClusterMember("foo")));
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a leader elect handler by method.
   */
  public void testRegisterAndUnregisterLeaderElectHandlerByMethod() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<LeaderElectEvent> handler = (event) -> succeeded.set(true);
    handlers.leaderElect().registerHandler(handler);
    handlers.leaderElect().handle(new LeaderElectEvent(1, new ClusterMember("foo")));
    assertTrue(succeeded.get());
    handlers.leaderElect().unregisterHandler(handler);
    succeeded.set(false);
    handlers.leaderElect().handle(new LeaderElectEvent(1, new ClusterMember("foo")));
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a leader elect handler by event.
   */
  public void testRegisterAndUnregisterLeaderElectHandlerByEvent() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<LeaderElectEvent> handler = (event) -> succeeded.set(true);
    handlers.event(LeaderElectEvent.class).registerHandler(handler);
    handlers.event(LeaderElectEvent.class)
      .handle(new LeaderElectEvent(1, new ClusterMember("foo")));
    assertTrue(succeeded.get());
    handlers.event(LeaderElectEvent.class).unregisterHandler(handler);
    succeeded.set(false);
    handlers.event(LeaderElectEvent.class)
      .handle(new LeaderElectEvent(1, new ClusterMember("foo")));
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a membership change handler by method.
   */
  public void testRegisterAndUnregisterMembershipChangeHandlerByMethod() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<MembershipChangeEvent> handler = (event) -> succeeded.set(true);
    handlers.membershipChange().registerHandler(handler);
    handlers.membershipChange().handle(new MembershipChangeEvent(null));
    assertTrue(succeeded.get());
    handlers.membershipChange().unregisterHandler(handler);
    succeeded.set(false);
    handlers.membershipChange().handle(new MembershipChangeEvent(null));
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a membership change handler by event.
   */
  public void testRegisterAndUnregisterMembershipChangeHandlerByEvent() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<MembershipChangeEvent> handler = (event) -> succeeded.set(true);
    handlers.event(MembershipChangeEvent.class).registerHandler(handler);
    handlers.event(MembershipChangeEvent.class).handle(new MembershipChangeEvent(null));
    assertTrue(succeeded.get());
    handlers.event(MembershipChangeEvent.class).unregisterHandler(handler);
    succeeded.set(false);
    handlers.event(MembershipChangeEvent.class).handle(new MembershipChangeEvent(null));
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a state change handler by method.
   */
  public void testRegisterAndUnregisterStateChangeHandlerByMethod() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<StateChangeEvent> handler = (event) -> succeeded.set(true);
    handlers.stateChange().registerHandler(handler);
    handlers.stateChange().handle(new StateChangeEvent(CopycatState.LEADER));
    assertTrue(succeeded.get());
    handlers.stateChange().unregisterHandler(handler);
    succeeded.set(false);
    handlers.stateChange().handle(new StateChangeEvent(CopycatState.LEADER));
    assertFalse(succeeded.get());
  }

  /**
   * Tests registering and unregistering a state change handler by event.
   */
  public void testRegisterAndUnregisterStateChangeHandlerByEvent() {
    AtomicBoolean succeeded = new AtomicBoolean();
    EventHandler<StateChangeEvent> handler = (event) -> succeeded.set(true);
    handlers.event(StateChangeEvent.class).registerHandler(handler);
    handlers.event(StateChangeEvent.class).handle(new StateChangeEvent(CopycatState.LEADER));
    assertTrue(succeeded.get());
    handlers.event(StateChangeEvent.class).unregisterHandler(handler);
    succeeded.set(false);
    handlers.event(StateChangeEvent.class).handle(new StateChangeEvent(CopycatState.LEADER));
    assertFalse(succeeded.get());
  }

}
