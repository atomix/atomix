/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

import java.net.URI;

import junit.framework.Assert;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.registry.impl.BasicRegistry;
import net.kuujo.copycat.uri.UriHost;
import net.kuujo.copycat.uri.UriInjector;
import net.kuujo.copycat.uri.UriPort;
import net.kuujo.copycat.uri.UriQueryParam;

import org.junit.Test;

/**
 * URI injector tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UriInjectorTest {

  @Test
  public void testInjectContext() throws Exception {
    CopyCatContext context = new CopyCatContext(new StateMachine(){});
    UriInjector injector = new UriInjector(new URI("http://localhost:8080"), context);
    TestInjectContext object = injector.inject(TestInjectContext.class);
    Assert.assertEquals(context, object.context);
  }

  public static class TestInjectContext {
    private CopyCatContext context;
  }

  @Test
  public void testInjectAnnotatedBean() throws Exception {
    UriInjector injector = new UriInjector(new URI("http://localhost:8080"), new CopyCatContext(new StateMachine(){}));
    TestInjectAnnotatedBean object = injector.inject(TestInjectAnnotatedBean.class);
    Assert.assertEquals("localhost", object.host);
    Assert.assertEquals(8080, object.port);
  }

  public static class TestInjectAnnotatedBean {
    private String host;
    private int port;

    @UriHost
    public void setHost(String host) {
      this.host = host;
    }

    public String getHost() {
      return host;
    }

    @UriPort
    public void setPort(int port) {
      this.port = port;
    }

    public int getPort() {
      return port;
    }
  }

  @Test
  public void testInjectUnannotatedBean() throws Exception {
    UriInjector injector = new UriInjector(new URI("http://localhost:8080?foo=baz&bar=1"), new CopyCatContext(new StateMachine(){}));
    TestInjectUnannotatedBean object = injector.inject(TestInjectUnannotatedBean.class);
    Assert.assertEquals("baz", object.foo);
    Assert.assertEquals(1, object.bar);
  }

  public static class TestInjectUnannotatedBean {
    private String foo;
    private int bar;

    public void setFoo(String foo) {
      this.foo = foo;
    }

    public String getFoo() {
      return foo;
    }

    public void setBar(int bar) {
      this.bar = bar;
    }

    public int getBar() {
      return bar;
    }
  }

  public static class TestObject {
    private final String foo;
    private final int bar;
    private TestObject(String foo, int bar) {
      this.foo = foo;
      this.bar = bar;
    }
  }

  @Test
  public void testInjectAnnotatedBeanFromRegistry() throws Exception {
    Registry registry = new BasicRegistry();
    registry.bind("test", new TestObject("Hello world!", 1000));
    UriInjector injector = new UriInjector(new URI("http://localhost:8080?registered=$test"), new CopyCatContext(new StateMachine(){}, new ClusterConfig(), registry));
    TestInjectAnnotatedBeanFromRegistry object = injector.inject(TestInjectAnnotatedBeanFromRegistry.class);
    Assert.assertNotNull(object.registered);
    Assert.assertEquals("Hello world!", object.registered.foo);
    Assert.assertEquals(1000, object.registered.bar);
  }

  public static class TestInjectAnnotatedBeanFromRegistry {
    private TestObject registered;

    @UriQueryParam("registered")
    public void setRegistered(TestObject registered) {
      this.registered = registered;
    }

    public TestObject getRegistered() {
      return registered;
    }
  }

  @Test
  public void testInjectUnannotatedBeanFromRegistry() throws Exception {
    Registry registry = new BasicRegistry();
    registry.bind("test", new TestObject("Hello world!", 1000));
    UriInjector injector = new UriInjector(new URI("http://localhost:8080?registered=$test"), new CopyCatContext(new StateMachine(){}, new ClusterConfig(), registry));
    TestInjectUnannotatedBeanFromRegistry object = injector.inject(TestInjectUnannotatedBeanFromRegistry.class);
    Assert.assertNotNull(object.registered);
    Assert.assertEquals("Hello world!", object.registered.foo);
    Assert.assertEquals(1000, object.registered.bar);
  }

  public static class TestInjectUnannotatedBeanFromRegistry {
    private TestObject registered;

    public void setRegistered(TestObject registered) {
      this.registered = registered;
    }

    public TestObject getRegistered() {
      return registered;
    }
  }

}
