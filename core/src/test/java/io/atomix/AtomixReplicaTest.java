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

import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.Commit;
import io.atomix.resource.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

/**
 * Replica test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixReplicaTest extends AbstractAtomixTest {

  @BeforeMethod
  protected void beforeMethod() {
    init();
  }

  @AfterMethod
  protected void afterMethod() throws Throwable {
    cleanup();
  }

  /**
   * Tests submitting a command.
   */
  public void testSubmitCommandWithSequentialConsistency() throws Throwable {
    testSubmitCommand(WriteConsistency.SEQUENTIAL_EVENT);
  }

  /**
   * Tests submitting a command.
   */
  public void testSubmitCommandWithAtomicConsistency() throws Throwable {
    testSubmitCommand(WriteConsistency.ATOMIC);
  }

  /**
   * Tests submitting a command with a configured consistency level.
   */
  private void testSubmitCommand(WriteConsistency consistency) throws Throwable {
    Atomix replica = createReplicas(8, 3, 1).iterator().next();

    TestResource resource = replica.get("test", TestResource.class).get(5, TimeUnit.SECONDS);

    resource.with(consistency).command("Hello world!").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(10000);
  }

  /**
   * Tests submitting a query.
   */
  public void testSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testSubmitQueryWithOptimizedAtomicConsistency() throws Throwable {
    testSubmitQuery(ReadConsistency.OPTIMIZED_ATOMIC);
  }

  /**
   * Tests submitting a query.
   */
  public void testSubmitQueryWithAtomicConsistency() throws Throwable {
    testSubmitQuery(ReadConsistency.ATOMIC);
  }

  /**
   * Tests submitting a query with a configured consistency level.
   */
  private void testSubmitQuery(ReadConsistency consistency) throws Throwable {
    Atomix replica = createReplicas(8, 3, 1).iterator().next();

    TestResource resource = replica.get("test", TestResource.class).get(5, TimeUnit.SECONDS);

    resource.with(consistency).query("Hello world!").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(10000);
  }

  /**
   * Tests submitting a command through all nodes.
   */
  public void testSubmitAll() throws Throwable {
    List<Atomix> replicas = createReplicas(8, 3, 1);

    for (Atomix replica : replicas) {
      ValueResource resource = replica.get("test", ValueResource.class).get(5, TimeUnit.SECONDS);
      resource.set("Hello world!").thenRun(this::resume);
      await(10000);
    }

    ValueResource resource = replicas.get(0).get("test", ValueResource.class).get(5, TimeUnit.SECONDS);
    resource.get().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests getting a resource and submitting commands.
   */
  public void testGetConcurrency() throws Throwable {
    List<Atomix> replicas = createReplicas(8, 3, 1);

    Atomix replica1 = replicas.get(0);
    Atomix replica2 = replicas.get(1);

    ValueResource resource1 = replica1.get("test", ValueResource.class).get(5, TimeUnit.SECONDS);
    ValueResource resource2 = replica2.get("test", ValueResource.class).get(5, TimeUnit.SECONDS);

    resource1.set("Hello world!").join();

    resource2.get().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests operating many separate resources from the same clients.
   */
  public void testOperateMany() throws Throwable {
    List<Atomix> replicas = createReplicas(8, 3, 1);

    Atomix replica1 = replicas.get(0);
    Atomix replica2 = replicas.get(1);

    ValueResource resource11 = replica1.get("test1", ValueResource.class).get(5, TimeUnit.SECONDS);
    ValueResource resource12 = replica2.get("test1", ValueResource.class).get(5, TimeUnit.SECONDS);
    ValueResource resource21 = replica1.get("test2", ValueResource.class).get(5, TimeUnit.SECONDS);
    ValueResource resource22 = replica2.get("test2", ValueResource.class).get(5, TimeUnit.SECONDS);

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
  @ResourceTypeInfo(id=3, stateMachine=TestStateMachine.class, typeResolver=TestResource.TypeResolver.class)
  public static class TestResource extends Resource<TestResource> {
    public TestResource(CopycatClient client, Resource.Options options) {
      super(client, options);
    }

    public CompletableFuture<String> command(String value) {
      return submit(new TestCommand(value));
    }

    public CompletableFuture<String> query(String value) {
      return submit(new TestQuery(value));
    }

    public static class TypeResolver implements SerializableTypeResolver {
      @Override
      public void resolve(SerializerRegistry registry) {

      }
    }
  }

  /**
   * Test state machine.
   */
  public static class TestStateMachine extends ResourceStateMachine {
    public TestStateMachine() {
      super(new ResourceType(TestResource.class));
    }

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
  @ResourceTypeInfo(id=4, stateMachine=ValueStateMachine.class, typeResolver=ValueResource.TypeResolver.class)
  public static class ValueResource extends Resource<ValueResource> {
    public ValueResource(CopycatClient client, Resource.Options options) {
      super(client, options);
    }

    public CompletableFuture<Void> set(String value) {
      return submit(new SetCommand(value));
    }

    public CompletableFuture<String> get() {
      return submit(new GetQuery());
    }

    public static class TypeResolver implements SerializableTypeResolver {
      @Override
      public void resolve(SerializerRegistry registry) {

      }
    }
  }

  /**
   * Value state machine.
   */
  public static class ValueStateMachine extends ResourceStateMachine {
    private Commit<SetCommand> value;

    public ValueStateMachine() {
      super(new ResourceType(ValueResource.class));
    }

    public void set(Commit<SetCommand> commit) {
      Commit<SetCommand> oldValue = value;
      value = commit;
      if (oldValue != null)
        oldValue.close();
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
