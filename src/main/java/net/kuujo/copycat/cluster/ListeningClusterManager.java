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

import java.util.Set;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;

/**
 * Base class for listening cluster managers.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ListeningClusterManager implements ClusterManager {
  protected final String address;
  protected final ClusterConfig config;
  protected final ClusterListener listener;

  private final Handler<String> joinHandler = new Handler<String>() {
    @Override
    public void handle(String address) {
      if (!address.equals(ListeningClusterManager.this.address)) {
        config.addRemoteMember(address);
      }
    }
  };

  private final Handler<String> leaveHandler = new Handler<String>() {
    @Override
    public void handle(String address) {
      if (!address.equals(ListeningClusterManager.this.address)) {
        config.removeRemoteMember(address);
      }
    }
  };

  public ListeningClusterManager(String address, ClusterConfig config, ClusterListener listener) {
    this.address = address;
    this.config = config;
    this.listener = listener;
  }

  @Override
  public ClusterConfig config() {
    return config;
  }

  @Override
  public ClusterManager start() {
    return start(null);
  }

  @Override
  public ClusterManager start(final Handler<AsyncResult<ClusterConfig>> doneHandler) {
    config.setLocalMember(address);
    listener.join(address, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<ClusterConfig>(result.cause()).setHandler(doneHandler);
        } else {
          listener.joinHandler(joinHandler);
          listener.leaveHandler(leaveHandler);
          listener.getMembers(new Handler<AsyncResult<Set<String>>>() {
            @Override
            public void handle(AsyncResult<Set<String>> result) {
              if (result.failed()) {
                new DefaultFutureResult<ClusterConfig>(result.cause()).setHandler(doneHandler);
              } else {
                for (String member : result.result()) {
                  if (!member.equals(address)) {
                    config.addRemoteMember(member);
                  }
                }
                new DefaultFutureResult<ClusterConfig>(config).setHandler(doneHandler);
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public void stop() {
    stop(null);
  }

  @Override
  public void stop(final Handler<AsyncResult<Void>> doneHandler) {
    config.setLocalMember(address);
    listener.leave(address, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          listener.joinHandler(null);
          listener.leaveHandler(null);
          new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
        }
      }
    });
  }

}
