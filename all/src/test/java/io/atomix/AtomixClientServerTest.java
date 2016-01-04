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
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.Commit;
import io.atomix.resource.Consistency;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceStateMachine;
import io.atomix.resource.ResourceTypeInfo;
import io.atomix.testing.AbstractAtomixTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

/**
 * Client server test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixClientServerTest extends AbstractAtomixTest {
  @BeforeMethod
  protected void beforeMethod() {
    init();
  }

  @AfterMethod
  protected void afterMethod() {
    cleanup();
  }

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
    createServers(5);

    Atomix client = createClient();

    TestResource resource = client.create("test", TestResource.class).get();

    resource.with(consistency).command("Hello world!").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(10000);
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
    createServers(5);

    Atomix client = createClient();

    TestResource resource = client.create("test", TestResource.class).get();

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
    createServers(5);

    Atomix client1 = createClient();
    Atomix client2 = createClient();

    ValueResource resource1 = client1.get("test", ValueResource.class).get();
    ValueResource resource2 = client2.get("test", ValueResource.class).get();

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
    createServers(5);

    Atomix client1 = createClient();
    Atomix client2 = createClient();

    ValueResource resource1 = client1.create("test", ValueResource.class).get();
    ValueResource resource2 = client2.create("test", ValueResource.class).get();

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
    createServers(5);

    Atomix client1 = createClient();
    Atomix client2 = createClient();

    ValueResource resource1 = client1.get("test", ValueResource.class).get();
    ValueResource resource2 = client2.create("test", ValueResource.class).get();

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
    createServers(5);
    Atomix client = createClient();

    client.keys().thenAccept(result -> {
      threadAssertTrue(result.isEmpty());
      resume();
    });
    await(10000);

    client.create("test", TestResource.class).get();
    client.keys().thenAccept(result -> {
      threadAssertTrue(result.size() == 1 && result.contains("test"));
      resume();
    });
    await(10000);

    client.create("value", ValueResource.class).get();
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
  @ResourceTypeInfo(id=1, stateMachine=TestStateMachine.class)
  public static class TestResource extends Resource<TestResource, Resource.Options> {

    public TestResource(CopycatClient client, Resource.Options options) {
      super(client, options);
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
  @ResourceTypeInfo(id=2, stateMachine=ValueStateMachine.class)
  public static class ValueResource extends Resource<ValueResource, Resource.Options> {

    public ValueResource(CopycatClient client, Resource.Options options) {
      super(client, options);
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
