package net.kuujo.copycat.event.impl;

import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandler;
import net.kuujo.copycat.event.LeaderElectEvent;

public class LeaderElectEventContext implements EventContext<LeaderElectEvent> {
  private EventHandler<LeaderElectEvent> handler;

  public void run(LeaderElectEvent event) {
    if (handler != null) {
      handler.handle(event);
    }
  }

  @Override
  public EventContext<LeaderElectEvent> run(EventHandler<LeaderElectEvent> handler) {
    this.handler = handler;
    return this;
  }

}
