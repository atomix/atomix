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
package net.kuujo.copycat.endpoint.impl;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.endpoint.EndpointException;
import net.kuujo.copycat.endpoint.EndpointFactory;
import net.kuujo.copycat.endpoint.EndpointUri;

/**
 * Bean based endpoint factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BeanEndpointFactory implements EndpointFactory {

  @Override
  public Endpoint createEndpoint(EndpointUri uri) {
    Class<? extends Endpoint> endpointClass = uri.getEndpointClass();
    Map<String, Object> args = uri.getEndpointArgs();
    Endpoint endpoint;
    try {
      endpoint = endpointClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new EndpointException(e);
    }

    try {
      BeanInfo info = Introspector.getBeanInfo(endpointClass);
      for (PropertyDescriptor property : info.getPropertyDescriptors()) {
        if (args.containsKey(property.getName())) {
          property.getWriteMethod().setAccessible(true);
          property.getWriteMethod().invoke(endpoint, property.getPropertyType().cast(args.get(property.getName())));
        }
      }
      return endpoint;
    } catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new EndpointException(e);
    }
  }

}
