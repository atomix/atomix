package net.kuujo.copycat.event.impl;

import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandler;
import net.kuujo.copycat.event.StopEvent;

public class StopEventContext implements EventContext<StopEvent> {
  private EventHandler<StopEvent> handler;

  public void run(StopEvent event) {
    if (handler != null) {
      handler.handle(event);
    }
  }

  @Override
  public EventContext<StopEvent> run(EventHandler<StopEvent> handler) {
    this.handler = handler;
    return this;
  }

}
