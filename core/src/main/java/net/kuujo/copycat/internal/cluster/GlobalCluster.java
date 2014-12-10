/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.internal.CopycatStateContext;
import net.kuujo.copycat.protocol.ConfigureRequest;
import net.kuujo.copycat.protocol.ConfigureResponse;
import net.kuujo.copycat.protocol.Request;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class GlobalCluster implements ManagedCluster, Observer {
  private final Protocol protocol;
  private final Router router;
  private final ExecutionContext executor;
  private final GlobalLocalMember localMember;
  private final Map<String, GlobalRemoteMember> remoteMembers = new HashMap<>();
  private final Set<String> updating = new HashSet<>();
  private final ClusterElection election;
  private CopycatStateContext context;
  private boolean open;

  public GlobalCluster(ClusterConfig config, Protocol protocol, Router router, ExecutionContext executor) {
    this.protocol = protocol;
    this.router = router;
    this.executor = executor;
    this.localMember = new GlobalLocalMember(config.getLocalMember(), protocol, executor);
    for (String uri : config.getRemoteMembers()) {
      this.remoteMembers.put(uri, new GlobalRemoteMember(uri, protocol, executor));
    }
    this.election = new ClusterElection(this, context);
  }

  /**
   * Sets the state context.
   */
  public GlobalCluster setState(CopycatStateContext state) {
    this.context = context;
    return this;
  }

  /**
   * Returns the state context.
   */
  public CopycatStateContext getState() {
    return context;
  }

  @Override
  public void update(Observable o, Object arg) {
    Iterator<Map.Entry<String, GlobalRemoteMember>> entryIterator = remoteMembers.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Map.Entry<String, GlobalRemoteMember> entry = entryIterator.next();
      if (!context.getMembers().contains(entry.getKey())) {
        entryIterator.remove();
        if (open) {
          entry.getValue().close();
        }
      }
    }

    for (String uri : context.getMembers()) {
      if (!localMember.uri().equals(uri) && !remoteMembers.containsKey(uri) && !updating.contains(uri)) {
        GlobalRemoteMember member = new GlobalRemoteMember(uri, protocol, executor);
        if (open) {
          updating.add(member.uri());
          member.open().whenComplete((result, error) -> {
            if (error == null) {
              remoteMembers.put(uri, member);
            }
            updating.remove(uri);
          });
        }
      }
    }
  }

  @Override
  public GlobalMember leader() {
    return context.getLeader() != null ? member(context.getLeader()) : null;
  }

  @Override
  public long term() {
    return context.getTerm();
  }

  @Override
  public Election election() {
    return election;
  }

  @Override
  public Set<Member> members() {
    Set<Member> members = new HashSet<>(remoteMembers.values());
    members.add(localMember);
    return members;
  }

  @Override
  public GlobalMember member(String uri) {
    GlobalMember member = remoteMembers.get(uri);
    if (member != null) {
      return member;
    } else if (localMember.uri().equals(uri)) {
      return localMember;
    }
    return null;
  }

  @Override
  public GlobalLocalMember localMember() {
    return localMember;
  }

  @Override
  public Set<Member> remoteMembers() {
    return new HashSet<>(remoteMembers.values());
  }

  @Override
  public GlobalMember remoteMember(String uri) {
    return remoteMembers.get(uri);
  }

  @Override
  public CompletableFuture<Cluster> configure(ClusterConfig configuration) {
    CompletableFuture<Cluster> future = new CompletableFuture<>();
    this.<ConfigureRequest, ConfigureResponse>send("configure", ConfigureRequest.builder()
      .withId(UUID.randomUUID().toString())
      .withMember(localMember.uri())
      .withMembers(configuration.getMembers())
      .build()).whenComplete((result, error) -> {
      if (error == null) {
        if (result.status().equals(Response.Status.OK)) {
          future.complete(this);
        } else {
          future.completeExceptionally(result.error());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Sends a message to the member of the given URI.
   */
  private <T extends Request, U extends Response> CompletableFuture<U> send(String topic, T request) {
    GlobalMember member = member(request.member());
    if (member != null) {
      return member.send(topic, 0, request);
    }
    CompletableFuture<U> future = new CompletableFuture<>();
    future.completeExceptionally(new IllegalStateException(String.format("Invalid member URI %s", request.member())));
    return future;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> open() {
    open = true;
    CompletableFuture<Void>[] futures = new CompletableFuture[remoteMembers.size() + 1];
    futures[0] = localMember.open();
    int i = 1;
    for (Map.Entry<String, GlobalRemoteMember> entry : remoteMembers.entrySet()) {
      futures[i++] = entry.getValue().open();
      i++;
    }
    return CompletableFuture.allOf(futures).thenRun(() -> {
      if (context instanceof Observable) {
        ((Observable) context).addObserver(this);
        ((Observable) context).addObserver(election);
      }
      router.createRoutes(this, context);
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    open = false;
    CompletableFuture<Void>[] futures = new CompletableFuture[remoteMembers.size() + 1];
    futures[0] = localMember.close();
    int i = 1;
    for (Map.Entry<String, GlobalRemoteMember> entry : remoteMembers.entrySet()) {
      futures[i++] = entry.getValue().close();
      i++;
    }
    router.destroyRoutes(this, context);
    if (context instanceof Observable) {
      ((Observable) context).deleteObserver(this);
      ((Observable) context).deleteObserver(election);
    }
    return CompletableFuture.allOf(futures);
  }

}
