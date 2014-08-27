package net.kuujo.copycat.event.impl;

import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandler;
import net.kuujo.copycat.event.StateChangeEvent;

public class StateChangeEventContext implements EventContext<StateChangeEvent> {
  private EventHandler<StateChangeEvent> handler;

  public void run(StateChangeEvent event) {
    if (handler != null) {
      handler.handle(event);
    }
  }

  @Override
  public EventContext<StateChangeEvent> run(EventHandler<StateChangeEvent> handler) {
    this.handler = handler;
    return this;
  }

}
