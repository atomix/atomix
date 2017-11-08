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

import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponse;
import org.jboss.resteasy.spi.ResourceFactory;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import java.util.function.Function;

/**
 * Vert.x REST resource factory.
 */
public class VertxRestResourceFactory implements ResourceFactory {
  private final Class<?> resourceClass;
  private final Function<PrimitiveCache, AbstractRestResource> resourceFactory;
  private final PrimitiveCache primitiveCache;

  public VertxRestResourceFactory(Class<?> resourceClass, Function<PrimitiveCache, AbstractRestResource> resourceFactory, PrimitiveCache primitiveCache) {
    this.resourceClass = resourceClass;
    this.resourceFactory = resourceFactory;
    this.primitiveCache = primitiveCache;
  }

  @Override
  public Class<?> getScannableClass() {
    return resourceClass;
  }

  @Override
  public void registered(ResteasyProviderFactory factory) {

  }

  @Override
  public Object createResource(HttpRequest request, HttpResponse response, ResteasyProviderFactory factory) {
    return resourceFactory.apply(primitiveCache);
  }

  @Override
  public void requestFinished(HttpRequest request, HttpResponse response, Object resource) {

  }

  @Override
  public void unregistered() {

  }
}
