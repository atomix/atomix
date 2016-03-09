/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.standalone.server;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.PropertiesReader;
import io.atomix.manager.ResourceServer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Standalone Server.
 * 
 * @author Jonathan Halterman
 */
public class StandaloneServer {
  public static void main(String[] args) throws Exception {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("AtomixServer")
        .defaultHelp(true)
        .description("Atomix server");
    parser.addArgument("-config").help("Atomix configuration file");
    parser.addArgument("-server").help("Server address in host:port format");
    parser.addArgument("-seed").help("Comma-separated list of seed node addresses in host:port format");

    Namespace ns = null;
    try {
      ns = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    ResourceServer.Builder builder = null;
    String config = ns.getString("config");

    if (config != null) {
      Properties properties = PropertiesReader.load(config).properties();
      builder = ResourceServer.builder(properties);
    } else {
      String server = ns.getString("server");
      String seed = ns.getString("seed");
      if (server == null || seed == null) {
        System.err.println("Must supply -config or -server and -seed");
        System.exit(1);
      }

      List<Address> seeds = Stream.of(seed.split(",")).map(a -> new Address(a)).collect(Collectors.toList());
      builder = ResourceServer.builder(new Address(server), seeds);
    }

    ResourceServer server = builder.build();
    server.start().join();

    while (server.isRunning()) {
      synchronized (StandaloneServer.class) {
        StandaloneServer.class.wait();
      }
    }
  }
}
