package net.kuujo.copycat.event.impl;

import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandler;
import net.kuujo.copycat.event.StartEvent;

public class StartEventContext implements EventContext<StartEvent> {
  private EventHandler<StartEvent> handler;

  public void run(StartEvent event) {
    if (handler != null) {
      handler.handle(event);
    }
  }

  @Override
  public EventContext<StartEvent> run(EventHandler<StartEvent> handler) {
    this.handler = handler;
    return this;
  }

}
