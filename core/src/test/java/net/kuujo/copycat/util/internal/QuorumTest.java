package net.kuujo.copycat.util.internal;

import static org.testng.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link Quorum}.
 * 
 * @author Jonathan Halterman
 */
@Test
public class QuorumTest {
  Quorum quorum;
  AtomicInteger callbackResult;
  
  @BeforeMethod
  protected void beforeMethod() {
    callbackResult = new AtomicInteger();
    quorum = new Quorum(3, r -> {
      callbackResult.set(r ? 1 : 2);
    });
  }
  
  public void shouldAcceptTrueOnSuccess() {
    quorum.succeed();
    quorum.succeed();
    quorum.fail();
    assertEquals(callbackResult.get(), 0);
    quorum.fail();
    quorum.succeed();
    assertEquals(callbackResult.get(), 1);
  }

  public void shouldAcceptFalseOnFail() {
    quorum.fail();
    quorum.fail();
    quorum.succeed();
    assertEquals(callbackResult.get(), 0);
    quorum.succeed();
    quorum.fail();
    assertEquals(callbackResult.get(), 2);
  }
}
