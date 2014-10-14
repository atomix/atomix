package net.kuujo.copycat.event;

import net.kuujo.copycat.internal.event.DefaultEventHandlers;
import net.kuujo.copycat.internal.event.DefaultEvents;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

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
  public void beforeMethod() {
    handlers = new DefaultEventHandlers();
    events = new DefaultEvents(handlers);
  }

  @AfterMethod
  public void afterMethod() {
    handlers = null;
    events = null;
  }

  /**
   * Tests running an event handler once.
   */
  public void testRunOnce() {
    AtomicInteger counter = new AtomicInteger();
    events.start().runOnce((event) -> counter.incrementAndGet());
    handlers.start().handle(new StartEvent());
    Assert.assertEquals(1, counter.get());
    handlers.start().handle(new StartEvent());
    Assert.assertEquals(1, counter.get());
    handlers.start().handle(new StartEvent());
    Assert.assertEquals(1, counter.get());
  }

  /**
   * Tests running an event handler many times.
   */
  public void testRunMany() {
    AtomicInteger counter = new AtomicInteger();
    events.start().run((event) -> counter.incrementAndGet());
    handlers.start().handle(new StartEvent());
    Assert.assertEquals(1, counter.get());
    handlers.start().handle(new StartEvent());
    Assert.assertEquals(2, counter.get());
    handlers.start().handle(new StartEvent());
    Assert.assertEquals(3, counter.get());
  }

}
