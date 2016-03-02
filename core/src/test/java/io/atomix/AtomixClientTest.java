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

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.Commit;
import io.atomix.resource.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client server test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixClientTest extends AbstractAtomixTest {

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
    createReplicas(5, 3, 1);

    Atomix client = createClient();

    TestResource resource = client.getResource("test", TestResource.class).get(5, TimeUnit.SECONDS);

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
  public void testSubmitQueryWithAtomicLeaseConsistency() throws Throwable {
    testSubmitQuery(ReadConsistency.ATOMIC_LEASE);
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
    createReplicas(5, 3, 1);

    Atomix client = createClient();

    TestResource resource = client.getResource("test", TestResource.class).get(5, TimeUnit.SECONDS);

    resource.with(consistency).query("Hello world!").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(10000);
  }

  /**
   * Tests getting a resource and submitting commands.
   */
  public void testGetConcurrency() throws Throwable {
    createReplicas(5, 3, 1);

    Atomix client1 = createClient();
    Atomix client2 = createClient();

    ValueResource resource1 = client1.getResource("test", ValueResource.class).get(5, TimeUnit.SECONDS);
    ValueResource resource2 = client2.getResource("test", ValueResource.class).get(5, TimeUnit.SECONDS);

    resource1.set("Hello world!").join();

    resource2.get().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests creating a resource and submitting commands.
   */
  public void testCreateConcurrency() throws Throwable {
    createReplicas(5, 3, 1);

    Atomix client1 = createClient();
    Atomix client2 = createClient();

    ValueResource resource1 = client1.getResource("test", ValueResource.class).get(5, TimeUnit.SECONDS);
    ValueResource resource2 = client2.getResource("test", ValueResource.class).get(5, TimeUnit.SECONDS);

    resource1.set("Hello world!").join();

    resource2.get().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests getting and creating a resource and submitting commands.
   */
  public void testGetCreateConcurrency() throws Throwable {
    createReplicas(5, 3, 1);

    Atomix client1 = createClient();
    Atomix client2 = createClient();

    ValueResource resource1 = client1.getResource("test", ValueResource.class).get(5, TimeUnit.SECONDS);
    ValueResource resource2 = client2.getResource("test", ValueResource.class).get(5, TimeUnit.SECONDS);

    resource1.set("Hello world!").join();

    resource2.get().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests getting resource keys.
   */
  public void testGetResourceKeys() throws Throwable {
    createReplicas(5, 3, 1);
    Atomix client = createClient();

    client.keys().thenAccept(result -> {
      threadAssertTrue(result.isEmpty());
      resume();
    });
    await(10000);

    client.getResource("test", TestResource.class).get(5, TimeUnit.SECONDS);
    client.keys().thenAccept(result -> {
      threadAssertTrue(result.size() == 1 && result.contains("test"));
      resume();
    });
    await(10000);

    client.getResource("value", ValueResource.class).get(5, TimeUnit.SECONDS);
    client.keys().thenAccept(result -> {
      threadAssertTrue(result.size() == 2 && result.contains("test") && result.contains("value"));
      resume();
    });
    await(10000);

    client.keys(TestResource.class).thenAccept(result -> {
      threadAssertTrue(result.size() == 1 && result.contains("test"));
      resume();
    });
    await(10000);

    client.keys(ValueResource.class).thenAccept(result -> {
      threadAssertTrue(result.size() == 1 && result.contains("value"));
      resume();
    });
    await(10000);
  }

  /**
   * Test resource.
   */
  @ResourceTypeInfo(id=1, factory=TestResource.Factory.class)
  public static class TestResource extends AbstractResource<TestResource> {

    public TestResource(CopycatClient client, Properties options) {
      super(client, options);
    }

    public CompletableFuture<String> command(String value) {
      return submit(new TestCommand(value));
    }

    public CompletableFuture<String> query(String value) {
      return submit(new TestQuery(value));
    }

    /**
     * Test resource factory.
     */
    static class Factory implements ResourceFactory<TestResource> {
      @Override
      public ResourceStateMachine createStateMachine(Properties config) {
        return new TestStateMachine(config);
      }

      @Override
      public TestResource createInstance(CopycatClient client, Properties options) {
        return new TestResource(client, options);
      }
    }
  }

  /**
   * Test state machine.
   */
  public static class TestStateMachine extends ResourceStateMachine {
    public TestStateMachine(Properties config) {
      super(config);
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
  @ResourceTypeInfo(id=2, factory=ValueResource.Factory.class)
  public static class ValueResource extends AbstractResource<ValueResource> {

    public ValueResource(CopycatClient client, Properties options) {
      super(client, options);
    }

    public CompletableFuture<Void> set(String value) {
      return submit(new SetCommand(value));
    }

    public CompletableFuture<String> get() {
      return submit(new GetQuery());
    }

    /**
     * Test resource factory.
     */
    static class Factory implements ResourceFactory<ValueResource> {
      @Override
      public ResourceStateMachine createStateMachine(Properties config) {
        return new TestStateMachine(config);
      }

      @Override
      public ValueResource createInstance(CopycatClient client, Properties options) {
        return new ValueResource(client, options);
      }
    }
  }

  /**
   * Value state machine.
   */
  public static class ValueStateMachine extends ResourceStateMachine {
    private Commit<SetCommand> value;

    public ValueStateMachine(Properties config) {
      super(config);
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
