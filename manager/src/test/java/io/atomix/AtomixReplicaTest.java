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
 * limitations under the License
 */
package io.atomix;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.Commit;
import io.atomix.resource.*;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.testng.Assert.assertEquals;

/**
 * Replica test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixReplicaTest extends AbstractReplicaTest {

  /**
   * Tests submitting a command.
   */
  public void testSubmitCommandWithNoneConsistency() throws Throwable {
    testSubmitCommand(Consistency.NONE);
  }

  /**
   * Tests submitting a command.
   */
  public void testSubmitCommandWithProcessConsistency() throws Throwable {
    testSubmitCommand(Consistency.PROCESS);
  }

  /**
   * Tests submitting a command.
   */
  public void testSubmitCommandWithSequentialConsistency() throws Throwable {
    testSubmitCommand(Consistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a command.
   */
  public void testSubmitCommandWithAtomicConsistency() throws Throwable {
    testSubmitCommand(Consistency.ATOMIC);
  }

  /**
   * Tests submitting a command with a configured consistency level.
   */
  private void testSubmitCommand(Consistency consistency) throws Throwable {
    Atomix replica = createReplicas(5).iterator().next();

    TestResource resource = replica.create("test", TestResource.TYPE).get();

    resource.with(consistency).command("Hello world!").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await();
  }

  /**
   * Tests submitting a query.
   */
  public void testSubmitQueryWithNoneConsistency() throws Throwable {
    testSubmitQuery(Consistency.NONE);
  }

  /**
   * Tests submitting a query.
   */
  public void testSubmitQueryWithProcessConsistency() throws Throwable {
    testSubmitQuery(Consistency.PROCESS);
  }

  /**
   * Tests submitting a query.
   */
  public void testSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(Consistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testSubmitQueryWithAtomicConsistency() throws Throwable {
    testSubmitQuery(Consistency.ATOMIC);
  }

  /**
   * Tests submitting a query with a configured consistency level.
   */
  private void testSubmitQuery(Consistency consistency) throws Throwable {
    Atomix replica = createReplicas(5).iterator().next();

    TestResource resource = replica.create("test", TestResource.TYPE).get();

    resource.with(consistency).query("Hello world!").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await();
  }

  /**
   * Tests getting a resource and submitting commands.
   */
  public void testGetConcurrency() throws Throwable {
    List<Atomix> replicas = createReplicas(5);

    Atomix replica1 = replicas.get(0);
    Atomix replica2 = replicas.get(1);

    ValueResource resource1 = replica1.get("test", ValueResource.TYPE).get();
    ValueResource resource2 = replica2.get("test", ValueResource.TYPE).get();

    resource1.set("Hello world!").join();

    resource2.get().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await();
  }

  /**
   * Tests creating a resource and submitting commands.
   */
  public void testCreateConcurrency() throws Throwable {
    List<Atomix> replicas = createReplicas(5);

    Atomix replica1 = replicas.get(0);
    Atomix replica2 = replicas.get(1);

    ValueResource resource1 = replica1.create("test", ValueResource.TYPE).get();
    ValueResource resource2 = replica2.create("test", ValueResource.TYPE).get();

    resource1.set("Hello world!").join();

    resource2.get().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await();
  }

  /**
   * Tests getting and creating a resource and submitting commands.
   */
  public void testGetCreateConcurrency() throws Throwable {
    List<Atomix> replicas = createReplicas(5);

    Atomix replica1 = replicas.get(0);
    Atomix replica2 = replicas.get(1);

    ValueResource resource1 = replica1.get("test", ValueResource.TYPE).get();
    ValueResource resource2 = replica2.create("test", ValueResource.TYPE).get();

    resource1.set("Hello world!").join();

    resource2.get().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await();
  }

  /**
   * Tests operating many separate resources from the same clients.
   */
  public void testOperateMany() throws Throwable {
    List<Atomix> replicas = createReplicas(5);

    Atomix replica1 = replicas.get(0);
    Atomix replica2 = replicas.get(1);

    ValueResource resource11 = replica1.get("test1", ValueResource.TYPE).get();
    ValueResource resource12 = replica2.create("test1", ValueResource.TYPE).get();
    ValueResource resource21 = replica1.get("test2", ValueResource.TYPE).get();
    ValueResource resource22 = replica2.create("test2", ValueResource.TYPE).get();

    resource11.set("foo").join();
    assertEquals(resource12.get().get(), "foo");

    resource21.set("bar").join();
    assertEquals(resource22.get().get(), "bar");

    assertEquals(resource11.get().get(), "foo");
    assertEquals(resource21.get().get(), "bar");
  }

  /**
   * Test resource.
   */
  @ResourceTypeInfo(id=3, stateMachine=TestStateMachine.class)
  public static class TestResource extends Resource {
    public static final ResourceType<TestResource> TYPE = new ResourceType<>(TestResource.class);

    public TestResource(CopycatClient client) {
      super(client);
    }

    @Override
    public ResourceType type() {
      return TYPE;
    }

    @Override
    public TestResource with(Consistency consistency) {
      super.with(consistency);
      return this;
    }

    public CompletableFuture<String> command(String value) {
      return submit(new TestCommand(value));
    }

    public CompletableFuture<String> query(String value) {
      return submit(new TestQuery(value));
    }
  }

  /**
   * Test state machine.
   */
  public static class TestStateMachine extends ResourceStateMachine {
    public String command(Commit<TestCommand> commit) {
      return commit.operation().value();
    }

    public String query(Commit<TestQuery> commit) {
      return commit.operation().value();
    }
  }

  /**
   * Test command.
   */
  public static class TestCommand implements Command<String> {
    private String value;

    public TestCommand(String value) {
      this.value = value;
    }

    public String value() {
      return value;
    }
  }

  /**
   * Test query.
   */
  public static class TestQuery implements Query<String> {
    private String value;

    public TestQuery(String value) {
      this.value = value;
    }

    public String value() {
      return value;
    }
  }

  /**
   * Value resource.
   */
  @ResourceTypeInfo(id=4, stateMachine=ValueStateMachine.class)
  public static class ValueResource extends Resource {
    public static final ResourceType<ValueResource> TYPE = new ResourceType<ValueResource>(ValueResource.class);

    public ValueResource(CopycatClient client) {
      super(client);
    }

    @Override
    public ResourceType type() {
      return TYPE;
    }

    public CompletableFuture<Void> set(String value) {
      return submit(new SetCommand(value));
    }

    public CompletableFuture<String> get() {
      return submit(new GetQuery());
    }
  }

  /**
   * Value state machine.
   */
  public static class ValueStateMachine extends ResourceStateMachine {
    private Commit<SetCommand> value;

    public void set(Commit<SetCommand> commit) {
      Commit<SetCommand> oldValue = value;
      value = commit;
      if (oldValue != null)
        oldValue.clean();
    }

    public String get(Commit<GetQuery> commit) {
      try {
        return value != null ? value.operation().value() : null;
      } finally {
        commit.close();
      }
    }
  }

  /**
   * Set command.
   */
  public static class SetCommand implements Command<Void> {
    private String value;

    public SetCommand(String value) {
      this.value = value;
    }

    public String value() {
      return value;
    }
  }

  /**
   * Get query.
   */
  public static class GetQuery implements Query<String> {
  }

}
