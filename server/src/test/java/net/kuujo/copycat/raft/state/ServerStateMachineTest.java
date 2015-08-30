/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.raft.state;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.Commit;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.StateMachineExecutor;
import net.kuujo.copycat.raft.protocol.Command;
import net.kuujo.copycat.raft.protocol.Query;
import net.kuujo.copycat.raft.storage.*;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.SingleThreadContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.*;

/**
 * Server state machine test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ServerStateMachineTest extends ConcurrentTestCase {
  private Context callerContext;
  private Context stateContext;
  private ServerStateMachine stateMachine;
  private long timestamp;
  private AtomicLong sequence;

  @BeforeMethod
  public void createStateMachine() {
    callerContext = new SingleThreadContext("caller", new Serializer());
    stateContext = new SingleThreadContext("state", new Serializer());
    stateMachine = new ServerStateMachine(new TestStateMachine(), i -> {}, stateContext);
    timestamp = System.currentTimeMillis();
    sequence = new AtomicLong();
  }

  /**
   * Tests registering a session.
   */
  public void testSessionRegisterKeepAlive() throws Throwable {
    callerContext.execute(() -> {

      RegisterEntry entry = new RegisterEntry()
        .setIndex(1)
        .setTerm(1)
        .setTimestamp(timestamp)
        .setTimeout(500)
        .setConnection(UUID.randomUUID());

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    ServerSession session = stateMachine.executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);

    callerContext.execute(() -> {

      KeepAliveEntry entry = new KeepAliveEntry()
        .setIndex(3)
        .setTerm(1)
        .setSession(1)
        .setTimestamp(timestamp + 1000)
        .setCommandSequence(0)
        .setEventSequence(0);

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    assertEquals(session.getTimestamp(), timestamp + 1000);
  }

  /**
   * Tests resetting session timeouts when a new leader is elected.
   */
  public void testSessionLeaderReset() throws Throwable {
    callerContext.execute(() -> {

      RegisterEntry entry = new RegisterEntry()
        .setIndex(1)
        .setTerm(1)
        .setTimestamp(timestamp)
        .setTimeout(500)
        .setConnection(UUID.randomUUID());

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    ServerSession session = stateMachine.executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);

    callerContext.execute(() -> {

      NoOpEntry entry = new NoOpEntry()
        .setIndex(2)
        .setTerm(1)
        .setTimestamp(timestamp + 100);

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    assertEquals(session.getTimestamp(), timestamp + 100);
  }

  /**
   * Tests expiring a session.
   */
  public void testSessionExpire() throws Throwable {
    callerContext.execute(() -> {

      RegisterEntry entry = new RegisterEntry()
        .setIndex(1)
        .setTerm(1)
        .setTimestamp(timestamp)
        .setTimeout(500)
        .setConnection(UUID.randomUUID());

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    ServerSession session = stateMachine.executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);

    callerContext.execute(() -> {

      KeepAliveEntry entry = new KeepAliveEntry()
        .setIndex(3)
        .setTerm(1)
        .setSession(2)
        .setTimestamp(timestamp + 1000)
        .setCommandSequence(0)
        .setEventSequence(0);

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertNotNull(error);
        resume();
      });
    });

    await();

    assertTrue(session.isExpired());
  }

  /**
   * Tests command sequencing.
   */
  public void testCommandSequence() throws Throwable {
    callerContext.execute(() -> {

      RegisterEntry entry = new RegisterEntry()
        .setIndex(1)
        .setTerm(1)
        .setTimestamp(timestamp)
        .setTimeout(500)
        .setConnection(UUID.randomUUID());

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    ServerSession session = stateMachine.executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);
    assertEquals(session.getVersion(), 1);
    assertEquals(session.getSequence(), 0);

    callerContext.execute(() -> {

      CommandEntry entry = new CommandEntry()
        .setIndex(2)
        .setTerm(1)
        .setSession(1)
        .setTimestamp(timestamp + 100)
        .setSequence(1)
        .setCommand(new TestCommand());

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertEquals(result, 1L);
        resume();
      });

    });

    await();

    assertEquals(session.getVersion(), 2);
    assertEquals(session.getSequence(), 1);
    assertEquals(session.getTimestamp(), timestamp + 100);

    callerContext.execute(() -> {

      CommandEntry entry = new CommandEntry()
        .setIndex(3)
        .setTerm(1)
        .setSession(1)
        .setTimestamp(timestamp + 200)
        .setSequence(3)
        .setCommand(new TestCommand());

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertEquals(result, 3L);
        resume();
      });

    });

    callerContext.execute(() -> {

      CommandEntry entry = new CommandEntry()
        .setIndex(4)
        .setTerm(1)
        .setSession(1)
        .setTimestamp(timestamp + 300)
        .setSequence(2)
        .setCommand(new TestCommand());

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertEquals(result, 2L);
        resume();
      });

    });

    await(1000, 2);

    assertEquals(session.getVersion(), 4);
    assertEquals(session.getSequence(), 3);
    assertEquals(session.getTimestamp(), timestamp + 300);
  }

  /**
   * Tests serializing queries.
   */
  public void testQuerySerialize() throws Throwable {
    callerContext.execute(() -> {

      RegisterEntry entry = new RegisterEntry()
        .setIndex(1)
        .setTerm(1)
        .setTimestamp(timestamp)
        .setTimeout(500)
        .setConnection(UUID.randomUUID());

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });

      threadAssertEquals(stateMachine.getLastApplied(), 1l);
    });

    await();

    ServerSession session = stateMachine.executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);
    assertEquals(session.getVersion(), 1);
    assertEquals(session.getSequence(), 0);

    callerContext.execute(() -> {

      QueryEntry entry = new QueryEntry()
        .setIndex(stateMachine.getLastApplied())
        .setTerm(1)
        .setSession(1)
        .setTimestamp(timestamp + 200)
        .setVersion(2)
        .setQuery(new TestQuery());

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertEquals(result, 2L);
        resume();
      });

    });

    callerContext.execute(() -> {

      CommandEntry entry = new CommandEntry()
        .setIndex(2)
        .setTerm(1)
        .setSession(1)
        .setTimestamp(timestamp + 100)
        .setSequence(1)
        .setCommand(new TestCommand());

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertEquals(result, 1L);
        resume();
      });

      threadAssertEquals(stateMachine.getLastApplied(), 2l);
    });

    await(1000, 2);

    assertEquals(session.getVersion(), 2);
    assertEquals(session.getSequence(), 1);
    assertEquals(session.getTimestamp(), timestamp + 100);
  }

  @AfterMethod
  public void closeStateMachine() {
    stateMachine.close();
    stateContext.close();
    callerContext.close();
  }

  /**
   * Test state machine.
   */
  private class TestStateMachine extends StateMachine {
    @Override
    public void configure(StateMachineExecutor executor) {
      executor.register(TestCommand.class, this::testCommand);
      executor.register(TestQuery.class, this::testQuery);
    }

    private long testCommand(Commit<TestCommand> commit) {
      return sequence.incrementAndGet();
    }

    private long testQuery(Commit<TestQuery> commit) {
      return sequence.incrementAndGet();
    }
  }

  /**
   * Test command.
   */
  private static class TestCommand implements Command<Long> {
  }

  /**
   * Test query.
   */
  private static class TestQuery implements Query<Long> {
  }

}
