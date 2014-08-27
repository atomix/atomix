package net.kuujo.copycat.event.impl;

import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandler;
import net.kuujo.copycat.event.MembershipChangeEvent;

public class MembershipChangeEventContext implements EventContext<MembershipChangeEvent> {
  private EventHandler<MembershipChangeEvent> handler;

  public void run(MembershipChangeEvent event) {
    if (handler != null) {
      handler.handle(event);
    }
  }

  @Override
  public EventContext<MembershipChangeEvent> run(EventHandler<MembershipChangeEvent> handler) {
    this.handler = handler;
    return this;
  }

}
