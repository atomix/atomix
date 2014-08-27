package net.kuujo.copycat.event.impl;

import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandler;
import net.kuujo.copycat.event.VoteCastEvent;

public class VoteCastEventContext implements EventContext<VoteCastEvent> {
  private EventHandler<VoteCastEvent> handler;

  public void run(VoteCastEvent event) {
    if (handler != null) {
      handler.handle(event);
    }
  }

  @Override
  public EventContext<VoteCastEvent> run(EventHandler<VoteCastEvent> handler) {
    this.handler = handler;
    return this;
  }

}
