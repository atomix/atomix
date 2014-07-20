package net.kuujo.copycat.protocol.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.kuujo.copycat.CopyCatContext;

class DirectProtocolRegistry {
  private static final Map<CopyCatContext, DirectProtocolRegistry> instances = new ConcurrentHashMap<>();
  private final Map<String, DirectProtocolServer> registry = new ConcurrentHashMap<>();

  static DirectProtocolRegistry getInstance(CopyCatContext context) {
    DirectProtocolRegistry registry = instances.get(context);
    if (registry == null) {
      registry = new DirectProtocolRegistry();
      instances.put(context, registry);
    }
    return registry;
  }

  void register(String address, DirectProtocolServer server) {
    registry.put(address, server);
  }

  void unregister(String address) {
    registry.remove(address);
  }

  DirectProtocolServer get(String address) {
    return registry.get(address);
  }

}
