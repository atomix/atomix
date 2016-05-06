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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.PropertiesReader;
import io.atomix.manager.ResourceServer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

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
    parser.addArgument("-address")
      .required(true)
      .metavar("HOST:PORT")
      .help("The server address");
    parser.addArgument("-bootstrap")
      .nargs("*")
      .metavar("HOST:PORT")
      .help("Bootstraps a new cluster");
    parser.addArgument("-join")
      .nargs("+")
      .metavar("HOST:PORT")
      .help("Joins an existing cluster");
    parser.addArgument("-config")
      .metavar("FILE")
      .help("Atomix configuration file");

    Namespace ns = null;
    try {
      ns = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    String address = ns.getString("address");
    String config = ns.getString("config");

    ResourceServer server;
    if (config != null) {
      Properties properties = PropertiesReader.load(config).properties();
      server = ResourceServer.builder(new Address(address), properties).build();
    } else {
      server = ResourceServer.builder(new Address(address)).build();
    }

    List<String> bootstrap = ns.getList("bootstrap");
    if (bootstrap != null) {
      List<Address> cluster = bootstrap.stream().map(Address::new).collect(Collectors.toList());
      server.bootstrap(cluster).join();
    } else {
      List<String> join = ns.getList("join");
      if (join != null) {
        List<Address> cluster = join.stream().map(Address::new).collect(Collectors.toList());
        server.join(cluster).join();
      } else {
        System.err.println("Must configure either -bootstrap or -join");
      }
    }

    synchronized (StandaloneServer.class) {
      while (server.isRunning()) {
        StandaloneServer.class.wait();
      }
    }
  }
}
