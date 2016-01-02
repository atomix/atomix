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
package io.atomix.resource;

import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.InstanceManager;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceStateMachine;
import io.atomix.resource.ResourceType;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

/**
 * Atomix instance manager test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class InstanceManagerTest {

  /**
   * Tests creating a resource.
   */
  public void testGetResource() throws Throwable {
    CopycatClient client = mock(CopycatClient.class);

    Resource<? extends Resource> resource = mockResource();

    InstanceManager manager = new InstanceManager(client);
    Resource instance1 = manager.get("test", resource.type()).get();
    Resource instance2 = manager.get("test", resource.type()).get();
    verify(resource).open();
    assertSame(instance1, instance2);
  }

  /**
   * Tests creating a resource.
   */
  public void testCreateResource() throws Throwable {
    CopycatClient client = mock(CopycatClient.class);

    Resource<? extends Resource> resource1 = mockResource();
    Resource<? extends Resource> resource2 = mockResource();

    InstanceManager manager = new InstanceManager(client);
    Resource instance1 = manager.create("test", resource1.type()).get();
    verify(resource1).open();
    assertSame(resource1, instance1);
    Resource instance2 = manager.create("test", resource2.type()).get();
    verify(resource2).open();
    assertSame(resource2, instance2);
    assertNotSame(instance1, instance2);
  }

  /**
   * Tests that resource are closed when the instance manager is closed.
   */
  public void testCloseResources() throws Throwable {
    CopycatClient client = mock(CopycatClient.class);

    Resource<? extends Resource> resource1 = mockResource();
    Resource<? extends Resource> resource2 = mockResource();

    InstanceManager manager = new InstanceManager(client);
    Resource instance1 = manager.get("test", resource1.type()).get();
    verify(resource1).open();
    assertSame(resource1, instance1);
    Resource instance2 = manager.create("test", resource2.type()).get();
    verify(resource2).open();
    assertSame(resource2, instance2);
    assertNotSame(instance1, instance2);

    manager.close().join();
    verify(resource1).close();
    verify(resource2).close();
  }

  /**
   * Returns a mock resource.
   */
  private Resource<? extends Resource> mockResource() {
    ResourceType<Resource> type = mock(ResourceType.class);

    Resource resource = mock(Resource.class);
    when(resource.type()).thenReturn(type);
    when(resource.open()).thenReturn(CompletableFuture.completedFuture(resource));
    when(resource.close()).thenReturn(CompletableFuture.completedFuture(null));

    when(type.id()).thenReturn(1);
    when(type.stateMachine()).thenReturn((Class) TestResourceStateMachine.class);
    when(type.factory()).thenReturn(c -> resource);
    return resource;
  }

  /**
   * Test resource state machine.
   */
  public static class TestResourceStateMachine extends ResourceStateMachine {
  }

}
