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
package net.kuujo.copycat.cluster;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import net.kuujo.copycat.util.AsyncAction;
import net.kuujo.copycat.util.AsyncExecutor;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;

import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MultiMap;

/**
 * Hazelcast cluster listener implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class HazelcastClusterListener implements ClusterListener, MembershipListener, EntryListener<String, String> {
  private final Vertx vertx;
  private final HazelcastInstance hazelcast;
  private final MultiMap<String, String> members;
  private Handler<String> joinHandler;
  private Handler<String> leaveHandler;
  private final AsyncExecutor executor = new AsyncExecutor(Executors.newSingleThreadExecutor());

  HazelcastClusterListener(String cluster, Vertx vertx, HazelcastInstance hazelcast) {
    this.vertx = vertx;
    this.hazelcast = hazelcast;
    hazelcast.getCluster().addMembershipListener(this);
    hazelcast.getConfig().getMultiMapConfig(cluster).addEntryListenerConfig(new EntryListenerConfig(this, true, true));
    this.members = hazelcast.getMultiMap(cluster);
  }

  @Override
  public void getMembers(Handler<AsyncResult<Set<String>>> resultHandler) {
    executor.execute(new AsyncAction<Set<String>>() {
      @Override
      public Set<String> execute() {
        Set<String> members = new HashSet<>();
        for (Map.Entry<String, String> entry : HazelcastClusterListener.this.members.entrySet()) {
          members.add(entry.getValue());
        }
        return members;
      }
    }, resultHandler);
  }

  @Override
  public synchronized void memberAdded(MembershipEvent event) {
    // Do nothing until the member address is added to the copycat map.
  }

  @Override
  public void memberAttributeChanged(MemberAttributeEvent event) {
    
  }

  @Override
  public synchronized void memberRemoved(MembershipEvent event) {
    String nodeID = event.getMember().getUuid();
    Collection<String> addresses = members.remove(nodeID);
    if (addresses != null && !addresses.isEmpty()) {
      for (final String address : addresses) {
        vertx.runOnContext(new Handler<Void>() {
          @Override
          public void handle(Void event) {
            if (leaveHandler != null) {
              leaveHandler.handle(address);
            }
          }
        });
      }
    }
  }

  @Override
  public synchronized void entryAdded(EntryEvent<String, String> event) {
    final String address = event.getValue();
    vertx.runOnContext(new Handler<Void>() {
      @Override
      public void handle(Void event) {
        if (joinHandler != null) {
          joinHandler.handle(address);
        }
      }
    });
  }

  @Override
  public void entryEvicted(EntryEvent<String, String> event) {
    
  }

  @Override
  public synchronized void entryRemoved(EntryEvent<String, String> event) {
    final String address = event.getValue();
    vertx.runOnContext(new Handler<Void>() {
      @Override
      public void handle(Void event) {
        if (leaveHandler != null) {
          leaveHandler.handle(address);
        }
      }
    });
  }

  @Override
  public void entryUpdated(EntryEvent<String, String> event) {
    
  }

  @Override
  public void join(final String address, Handler<AsyncResult<Void>> doneHandler) {
    executor.execute(new AsyncAction<Void>() {
      @Override
      public Void execute() {
        members.put(hazelcast.getCluster().getLocalMember().getUuid(), address);
        return null;
      }
    }, doneHandler);
  }

  @Override
  public void joinHandler(Handler<String> handler) {
    joinHandler = handler;
  }

  @Override
  public void leave(final String address, Handler<AsyncResult<Void>> doneHandler) {
    executor.execute(new AsyncAction<Void>() {
      @Override
      public Void execute() {
        members.remove(hazelcast.getCluster().getLocalMember().getUuid(), address);
        return null;
      }
    }, doneHandler);
  }

  @Override
  public void leaveHandler(Handler<String> handler) {
    leaveHandler = handler;
  }

}
