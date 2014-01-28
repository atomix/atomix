package net.kuujo.raft.state;

import java.util.ArrayList;
import java.util.List;

import org.vertx.java.core.Handler;

public class StateLock {
  private final List<Handler<Void>> handlers = new ArrayList<>();
  private boolean locked;

  public void acquire(Handler<Void> handler) {
    if (!locked) {
      locked = true;
      handler.handle((Void) null);
    }
    else {
      handlers.add(handler);
    }
  }

  public void release() {
    if (!handlers.isEmpty()) {
      handlers.remove(0).handle((Void) null);
    }
    else {
      locked = false;
    }
  }

}
