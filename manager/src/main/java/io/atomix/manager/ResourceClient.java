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
package io.atomix.manager;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.ResourceRegistry;

import java.util.Arrays;
import java.util.Collection;

/**
 * Provides an interface for creating and operating on {@link io.atomix.resource.Resource}s remotely.
 * <p>
 * This {@link ResourceManager} implementation facilitates working with {@link io.atomix.resource.Resource}s remotely as
 * a client of the Atomix cluster. To create a client, construct a client builder via {@link #builder(Address...)}.
 * The builder requires a list of {@link Address}es to which to connect.
 * <pre>
 *   {@code
 *   List<Address> servers = Arrays.asList(
 *     new Address("123.456.789.0", 5000),
 *     new Address("123.456.789.1", 5000)
 *   );
 *   Atomix atomix = AtomixClient.builder(servers)
 *     .withTransport(new NettyTransport())
 *     .build();
 *   }
 * </pre>
 * The {@link Address} list does not have to include all servers in the cluster, but must include at least one live
 * server in order for the client to connect. Once the client connects to the cluster and opens a session, the client
 * will receive an updated list of servers to which to connect.
 * <p>
 * Clients communicate with the cluster via a {@link Transport}. By default, the {@code NettyTransport} is used if
 * no transport is explicitly configured. Thus, if no transport is configured then the Netty transport is expected
 * to be available on the classpath.
 * <p>
 * <b>Client lifecycle</b>
 * <p>
 * When a client is {@link #open() started}, the client will attempt to contact random servers in the provided
 * {@link Address} list to open a new session. Opening a client session requires only that the client be able to
 * communicate with at least one server which can communicate with the leader. Once a session has been opened,
 * the client will periodically send keep-alive requests to the cluster to maintain its session. In the event
 * that the client crashes or otherwise becomes disconnected from the cluster, the client's session will expire
 * after a configured session timeout and the client will have to open a new session to reconnect.
 * <p>
 * Clients may connect to and communicate with any server in the cluster. Typically, once a client connects to a
 * server, the client will attempt to remain connected to that server until some exceptional case occurs. Exceptional
 * cases may include a failure of the server, a network partition, or the server falling too far out of sync with
 * the rest of the cluster. When a failure in the cluster occurs and the client becomes disconnected from the cluster,
 * it will transparently reconnect to random servers until it finds a reachable server.
 * <p>
 * During certain cluster events such as leadership changes, the client may not be able to communicate with the
 * cluster for some arbitrary (but typically short) period of time. During that time, Atomix guarantees that the
 * client's session will not expire even if its timeout elapses. Once a new leader is elected, the client's session
 * timeout is reset.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class ResourceClient extends ResourceManager {

  /**
   * Returns a new Atomix client builder.
   * <p>
   * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member.
   *
   * @param members The cluster members to which to connect.
   * @return The client builder.
   */
  public static Builder builder(Address... members) {
    return new Builder(Arrays.asList(Assert.notNull(members, "members")));
  }

  /**
   * Returns a new Atomix client builder.
   * <p>
   * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member.
   *
   * @param members The cluster members to which to connect.
   * @return The client builder.
   */
  public static Builder builder(Collection<Address> members) {
    return new Builder(members);
  }

  /**
   * @throws NullPointerException if {@code factory} is null
   */
  public ResourceClient(CopycatClient client, ResourceRegistry registry) {
    super(client, registry);
  }

  /**
   * Builds an {@link ResourceClient}.
   * <p>
   * The client builder configures an {@link ResourceClient} to connect to a cluster of {@link ResourceServer}s
   * or {@link ResourceReplica}. To create a client builder, use the {@link #builder(Address...)} method.
   * <pre>
   *   {@code
   *   Atomix client = AtomixClient.builder(servers)
   *     .withTransport(new NettyTransport())
   *     .build();
   *   }
   * </pre>
   */
  public static class Builder extends ResourceManager.Builder {
    private Transport transport;

    private Builder(Collection<Address> members) {
      super(members);
    }

    @Override
    public ResourceManager.Builder withTransport(Transport transport) {
      this.transport = transport;
      return super.withTransport(transport);
    }

    @Override
    public ResourceClient build() {
      if (transport == null) {
        try {
          transport = (Transport) Class.forName("io.atomix.catalyst.transport.NettyTransport").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new ConfigurationException("transport not configured");
        }
      }

      // Create a resource registry and resolve resources with the configured resolver.
      ResourceRegistry registry = new ResourceRegistry();
      resourceResolver.resolve(registry);

      return new ResourceClient(clientBuilder.build(), registry);
    }
  }

}
