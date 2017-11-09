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
package io.atomix.rest.impl;

import io.atomix.cluster.ClusterService;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponse;
import org.jboss.resteasy.spi.ResourceFactory;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

/**
 * Atomix resource factory.
 */
public class AtomixResourceFactory implements ResourceFactory {
  private final ClusterService clusterService;
  private final PrimitiveCache primitiveCache;

  public AtomixResourceFactory(ClusterService clusterService, PrimitiveCache primitiveCache) {
    this.clusterService = clusterService;
    this.primitiveCache = primitiveCache;
  }

  @Override
  public Class<?> getScannableClass() {
    return AtomixResource.class;
  }

  @Override
  public void registered(ResteasyProviderFactory resteasyProviderFactory) {

  }

  @Override
  public Object createResource(HttpRequest httpRequest, HttpResponse httpResponse, ResteasyProviderFactory resteasyProviderFactory) {
    return new AtomixResource(clusterService, primitiveCache);
  }

  @Override
  public void requestFinished(HttpRequest httpRequest, HttpResponse httpResponse, Object o) {

  }

  @Override
  public void unregistered() {

  }
}
