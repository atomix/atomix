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

import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.DynamicClusterConfig;
import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.InstallResponse;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.ProtocolUri;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * Cluster context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterContext implements Observer {
  private final CopyCatContext context;
  private final DynamicClusterConfig config;
  private ProtocolServer server;
  private final Map<String, ProtocolClient> clients = new HashMap<>();

  public ClusterContext(DynamicClusterConfig config, CopyCatContext context) {
    this.context = context;
    this.config = config;
    this.config.addObserver(this);
    clusterChanged(this.config);
  }

  @Override
  public void update(Observable o, Object arg) {
    clusterChanged((ClusterConfig) o);
  }

  /**
   * Returns the internal cluster configuration.
   *
   * @return The internal replica cluster configuration.
   */
  public DynamicClusterConfig config() {
    return config;
  }

  /**
   * Called when an observable cluster configuration has changed.
   */
  private void clusterChanged(ClusterConfig config) {
    if (server == null && this.config.getLocalMember() != null) {
      ProtocolUri uri = new ProtocolUri(this.config.getLocalMember());
      Class<? extends ProtocolServer> serverClass = uri.getServerClass();
      try {
        server = serverClass.newInstance();
        server.init(context);
      } catch (InstantiationException | IllegalAccessException e) {
        // Log the exception.
      }
    }

    // Always ensure that the local clients are up to date based on the cluster configuration.
    for (String member : this.config.getMembers()) {
      if (!clients.containsKey(member)) {
        ProtocolUri uri = new ProtocolUri(member);
        Class<? extends ProtocolClient> clientClass = uri.getClientClass();
        try {
          ProtocolClient client = clientClass.newInstance();
          client.init(context);
          clients.put(member, client);
        } catch (InstantiationException | IllegalAccessException e) {
          // Perhaps log the exception.
        }
      }
    }
  }

  /**
   * Pings the replica at the given address.
   *
   * @param address The address of the replica to ping.
   * @param request The ping request.
   * @param callback An asynchronous callback to be called once complete.
   */
  void ping(String address, PingRequest request, AsyncCallback<PingResponse> callback) {
    ProtocolClient client = clients.get(address);
    if (client != null) {
      client.ping(request, callback);
    } else {
      callback.fail(new CopyCatException("Invalid replica address " + address));
    }
  }

  /**
   * Registers a callback to be called when a ping request is received.
   *
   * @param callback A callback to be called when a ping request is received.
   */
  void pingCallback(AsyncCallback<PingRequest> callback) {
    server.pingCallback(callback);
  }

  /**
   * Synchronizes the replica at the given address.
   *
   * @param address The address of the replica to synchronize.
   * @param request The sync request.
   * @param callback An asynchronous callback to be called once complete.
   */
  void sync(String address, SyncRequest request, AsyncCallback<SyncResponse> callback) {
    ProtocolClient client = clients.get(address);
    if (client != null) {
      client.sync(request, callback);
    } else {
      callback.fail(new CopyCatException("Invalid replica address " + address));
    }
  }

  /**
   * Registers a callback to be called when a sync request is received.
   *
   * @param callback A callback to be called when a sync request is received.
   */
  void syncCallback(AsyncCallback<SyncRequest> callback) {
    server.syncCallback(callback);
  }

  /**
   * Installs a snapshot on the replica at the given address.
   *
   * @param address The address of the replica to which to install the snapshot.
   * @param request The install request.
   * @param callback An asynchronous callback to be called once complete.
   */
  void install(String address, InstallRequest request, AsyncCallback<InstallResponse> callback) {
    ProtocolClient client = clients.get(address);
    if (client != null) {
      client.install(request, callback);
    } else {
      callback.fail(new CopyCatException("Invalid replica address " + address));
    }
  }

  /**
   * Registers a callback to be called when an install request is received.
   *
   * @param callback A callback to be called when an install request is received.
   */
  void installCallback(AsyncCallback<InstallRequest> callback) {
    server.installCallback(callback);
  }

  /**
   * Polls the replica at the given address.
   *
   * @param address The address of the replica to poll.
   * @param request The poll request.
   * @param callback An asynchronous callback to be called once complete.
   */
  void poll(String address, PollRequest request, AsyncCallback<PollResponse> callback) {
    ProtocolClient client = clients.get(address);
    if (client != null) {
      client.poll(request, callback);
    } else {
      callback.fail(new CopyCatException("Invalid replica address " + address));
    }
  }

  /**
   * Registers a callback to be called when a poll request is received.
   *
   * @param callback A callback to be called when a poll request is received.
   */
  void pollCallback(AsyncCallback<PollRequest> callback) {
    server.pollCallback(callback);
  }

  /**
   * Submits a command to the replica at the given address.
   *
   * @param address The address of the replica to which to submit the command.
   * @param request The submit request.
   * @param callback An asynchronous callback to be called once complete.
   */
  void submit(String address, SubmitRequest request, AsyncCallback<SubmitResponse> callback) {
    ProtocolClient client = clients.get(address);
    if (client != null) {
      client.submit(request, callback);
    } else {
      callback.fail(new CopyCatException("Invalid replica address " + address));
    }
  }

  /**
   * Registers a callback to be called when a submit request is received.
   *
   * @param callback A callback to be called when a submit request is received.
   */
  void submitCallback(AsyncCallback<SubmitRequest> callback) {
    server.submitCallback(callback);
  }

  /**
   * Starts the cluster context.
   *
   * @param callback A callback to be called once the context is started.
   */
  public void start(AsyncCallback<Void> callback) {
    if (server == null) {
      callback.fail(new CopyCatException("No local member defined"));
    } else {
      server.start(callback);
    }
  }

  /**
   * Staps the cluste context.
   *
   * @param callback A callback to be called once the context is stopped.
   */
  public void stop(AsyncCallback<Void> callback) {
    if (server != null) {
      server.stop(callback);
    }
  }

}
