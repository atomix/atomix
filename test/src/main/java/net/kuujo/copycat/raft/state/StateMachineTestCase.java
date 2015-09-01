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
import net.kuujo.copycat.io.storage.Entry;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.protocol.Command;
import net.kuujo.copycat.raft.protocol.Query;
import net.kuujo.copycat.raft.session.Session;
import net.kuujo.copycat.raft.storage.CommandEntry;
import net.kuujo.copycat.raft.storage.KeepAliveEntry;
import net.kuujo.copycat.raft.storage.QueryEntry;
import net.kuujo.copycat.raft.storage.RegisterEntry;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.SingleThreadContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.*;

/**
 * State machine test case.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class StateMachineTestCase extends ConcurrentTestCase {
  private Context callerContext;
  private Context stateContext;
  private ServerStateMachine stateMachine;
  private Map<Long, Long> sequence;
  private Set<Long> cleaned;

  @BeforeMethod
  public void setupStateMachine() {
    sequence = new HashMap<>();
    cleaned = new HashSet<>();
    callerContext = new SingleThreadContext("caller", new Serializer());
    stateContext = new SingleThreadContext("state", new Serializer());
    stateMachine = new ServerStateMachine(createStateMachine(), cleaned::add, stateContext);
  }

  /**
   * Creates a new state machine.
   */
  protected abstract StateMachine createStateMachine();

  /**
   * Asserts that the given index has been cleaned from the log.
   */
  protected void assertCleaned(long index) {
    assertTrue(cleaned.contains(index));
  }

  /**
   * Asserts that the given index has not been cleaned from the log.
   */
  protected void assertNotCleaned(long index) {
    assertFalse(cleaned.contains(index));
  }

  /**
   * Registers a new session.
   */
  protected Session register(long index, long timestamp) throws Throwable {
    callerContext.execute(() -> {

      RegisterEntry entry = new RegisterEntry()
        .setIndex(index)
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

    ServerSession session = stateMachine.executor().context().sessions().getSession(index);
    assertNotNull(session);
    assertEquals(session.id(), index);
    assertEquals(session.getTimestamp(), timestamp);

    return session;
  }

  /**
   * Keeps a session alive.
   */
  protected Session keepAlive(long index, long timestamp, Session session) throws Throwable {
    callerContext.execute(() -> {

      KeepAliveEntry entry = new KeepAliveEntry()
        .setIndex(index)
        .setTerm(1)
        .setSession(session.id())
        .setTimestamp(timestamp)
        .setCommandSequence(0)
        .setEventSequence(0);

      stateMachine.apply(entry).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    assertEquals(((ServerSession) session).getTimestamp(), timestamp);
    return session;
  }

  /**
   * Applies the given command to the state machine.
   *
   * @param session The session for which to apply the command.
   * @param command The command to apply.
   * @return The command output.
   */
  protected <T> T apply(long index, long timestamp, Session session, Command<T> command) throws Throwable {
    Long sequence = this.sequence.get(session.id());
    sequence = sequence != null ? sequence + 1 : 1;
    this.sequence.put(session.id(), sequence);

    CommandEntry entry = new CommandEntry()
      .setIndex(index)
      .setTerm(1)
      .setSession(session.id())
      .setTimestamp(timestamp)
      .setSequence(sequence)
      .setCommand(command);

    return apply(entry);
  }

  /**
   * Applies the given query to the state machine.
   *
   * @param session The session for which to apply the query.
   * @param query The query to apply.
   * @return The query output.
   */
  protected <T> T apply(long timestamp, Session session, Query<T> query) throws Throwable {
    QueryEntry entry = new QueryEntry()
      .setIndex(stateMachine.getLastApplied())
      .setTerm(1)
      .setSession(session.id())
      .setTimestamp(timestamp)
      .setVersion(0)
      .setQuery(query);

    return apply(entry);
  }

  /**
   * Applies the given entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The entry output.
   */
  @SuppressWarnings("unchecked")
  private <T> T apply(Entry entry) throws Throwable {
    AtomicReference<Object> reference = new AtomicReference<>();
    callerContext.execute(() -> stateMachine.apply(entry).whenComplete((result, error) -> {
      if (error == null) {
        reference.set(result);
      } else {
        reference.set(error);
      }
      resume();
    }));

    await();

    Object result = reference.get();
    if (result instanceof Throwable) {
      throw (Throwable) result;
    }
    return (T) result;
  }

  @AfterMethod
  public void teardownStateMachine() {
    stateMachine.close();
    stateContext.close();
    callerContext.close();
  }

}
