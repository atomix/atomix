/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft.impl;

import io.atomix.protocols.raft.service.ServiceRevision;
import io.atomix.protocols.raft.service.impl.DefaultServiceContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Raft service registry.
 */
public class RaftServiceRegistry implements Iterable<DefaultServiceContext> {
  private final Map<String, List<DefaultServiceContext>> services = new ConcurrentHashMap<>();

  /**
   * Registers a new service.
   *
   * @param service the service to register
   */
  public void registerService(DefaultServiceContext service) {
    List<DefaultServiceContext> revisions = services.computeIfAbsent(service.serviceName(), name -> new CopyOnWriteArrayList<>());
    revisions.removeIf(revision -> revision.revision().revision() == service.revision().revision());
    revisions.add(service);
  }

  /**
   * Gets a registered service by revision.
   *
   * @param name     the service name
   * @param revision the service revision
   * @return the registered service
   */
  public DefaultServiceContext getRevision(String name, ServiceRevision revision) {
    List<DefaultServiceContext> revisions = services.get(name);
    return revisions != null
        ? revisions.stream()
        .filter(s -> s.revision().revision() == revision.revision())
        .findFirst()
        .orElse(null)
        : null;
  }

  /**
   * Gets the current revision for a registered service.
   *
   * @param name the service name
   * @return the current revision for the given service
   */
  public DefaultServiceContext getCurrentRevision(String name) {
    List<DefaultServiceContext> revisions = services.get(name);
    return revisions != null && !revisions.isEmpty() ? revisions.get(revisions.size() - 1) : null;
  }

  /**
   * Returns the revision prior to the given revision or null.
   *
   * @param name     the service name
   * @param revision the revision number
   * @return the service context
   */
  public DefaultServiceContext getPreviousRevision(String name, ServiceRevision revision) {
    List<DefaultServiceContext> revisions = services.get(name);
    for (int i = 0; i < revisions.size(); i++) {
      DefaultServiceContext service = revisions.get(i);
      if (service.revision().revision() == revision.revision()) {
        if (i > 0) {
          return revisions.get(i - 1);
        }
        return null;
      }
    }
    return null;
  }

  /**
   * Returns the revision following to the given revision or null.
   *
   * @param name     the service name
   * @param revision the revision number
   * @return the service context
   */
  public DefaultServiceContext getNextRevision(String name, ServiceRevision revision) {
    List<DefaultServiceContext> revisions = services.get(name);
    for (int i = 0; i < revisions.size(); i++) {
      DefaultServiceContext service = revisions.get(i);
      if (service.revision().revision() == revision.revision()) {
        if (i < revisions.size() - 1) {
          return revisions.get(i + 1);
        }
        return null;
      }
    }
    return null;
  }

  /**
   * Returns all revisions for the given service.
   *
   * @param name the service name
   * @return a collection of revisions for the given service
   */
  public Collection<DefaultServiceContext> getRevisions(String name) {
    List<DefaultServiceContext> revisions = services.get(name);
    return revisions != null ? revisions : Collections.emptyList();
  }

  @Override
  public Iterator<DefaultServiceContext> iterator() {
    return services.values().stream()
        .flatMap(s -> s.stream())
        .iterator();
  }
}
