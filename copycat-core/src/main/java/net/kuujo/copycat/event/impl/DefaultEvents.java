package net.kuujo.copycat.event.impl;

import net.kuujo.copycat.event.Event;
import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.Events;
import net.kuujo.copycat.event.LeaderElectEvent;
import net.kuujo.copycat.event.MembershipChangeEvent;
import net.kuujo.copycat.event.StartEvent;
import net.kuujo.copycat.event.StateChangeEvent;
import net.kuujo.copycat.event.StopEvent;
import net.kuujo.copycat.event.VoteCastEvent;

public class DefaultEvents implements Events {
  private final StartEventContext start = new StartEventContext();
  private final StopEventContext stop = new StopEventContext();
  private final VoteCastEventContext voteCast = new VoteCastEventContext();
  private final LeaderElectEventContext leaderElect = new LeaderElectEventContext();
  private final StateChangeEventContext stateChange = new StateChangeEventContext();
  private final MembershipChangeEventContext membershipChange = new MembershipChangeEventContext();

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Event> EventContext<T> event(Class<T> event) {
    if (event == StartEvent.class) {
      return (EventContext<T>) start();
    } else if (event == StopEvent.class) {
      return (EventContext<T>) stop();
    } else if (event == VoteCastEvent.class) {
      return (EventContext<T>) voteCast();
    } else if (event == LeaderElectEvent.class) {
      return (EventContext<T>) leaderElect();
    } else if (event == StateChangeEvent.class) {
      return (EventContext<T>) stateChange();
    } else if (event == MembershipChangeEvent.class) {
      return (EventContext<T>) membershipChange();
    } else {
      throw new UnsupportedOperationException("Unsupported event type");
    }
  }

  @Override
  public StartEventContext start() {
    return start;
  }

  @Override
  public StopEventContext stop() {
    return stop;
  }

  @Override
  public VoteCastEventContext voteCast() {
    return voteCast;
  }

  @Override
  public LeaderElectEventContext leaderElect() {
    return leaderElect;
  }

  @Override
  public StateChangeEventContext stateChange() {
    return stateChange;
  }

  @Override
  public MembershipChangeEventContext membershipChange() {
    return membershipChange;
  }

}
