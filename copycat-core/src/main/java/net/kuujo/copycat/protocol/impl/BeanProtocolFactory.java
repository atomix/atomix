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
package net.kuujo.copycat.protocol.impl;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.ProtocolFactory;
import net.kuujo.copycat.protocol.ProtocolUri;

/**
 * Bean based protocol factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BeanProtocolFactory implements ProtocolFactory {

  @Override
  public Protocol createProtocol(ProtocolUri uri) {
    Class<? extends Protocol> protocolClass = uri.getProtocolClass();
    Map<String, Object> args = uri.getProtocolArgs();
    Protocol protocol;
    try {
      protocol = protocolClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ProtocolException(e);
    }

    try {
      BeanInfo info = Introspector.getBeanInfo(protocolClass);
      for (PropertyDescriptor property : info.getPropertyDescriptors()) {
        if (args.containsKey(property.getName())) {
          property.getWriteMethod().setAccessible(true);
          property.getWriteMethod().invoke(protocol, property.getPropertyType().cast(args.get(property.getName())));
        }
      }
      return protocol;
    } catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new ProtocolException(e);
    }
  }

}
