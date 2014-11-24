package net.kuujo.copycat.event;

import static org.testng.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import net.kuujo.copycat.internal.event.DefaultEventHandlers;
import net.kuujo.copycat.internal.event.DefaultEvents;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Events context test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class EventContextTest {
  private DefaultEventHandlers handlers;
  private Events events;

  @BeforeMethod
  protected void beforeMethod() {
    handlers = new DefaultEventHandlers();
    events = new DefaultEvents(handlers);
  }

  /**
   * Tests running an event handler once.
   */
  public void testRunOnce() {
    AtomicInteger counter = new AtomicInteger();
    events.start().runOnce((event) -> counter.incrementAndGet());
    handlers.start().handle(new StartEvent());
    assertEquals(1, counter.get());
    handlers.start().handle(new StartEvent());
    assertEquals(1, counter.get());
    handlers.start().handle(new StartEvent());
    assertEquals(1, counter.get());
  }

  /**
   * Tests running an event handler many times.
   */
  public void testRunMany() {
    AtomicInteger counter = new AtomicInteger();
    events.start().run((event) -> counter.incrementAndGet());
    handlers.start().handle(new StartEvent());
    assertEquals(1, counter.get());
    handlers.start().handle(new StartEvent());
    assertEquals(2, counter.get());
    handlers.start().handle(new StartEvent());
    assertEquals(3, counter.get());
  }

}
