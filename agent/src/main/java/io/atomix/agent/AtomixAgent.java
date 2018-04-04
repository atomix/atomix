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
package io.atomix.agent;

import io.atomix.cluster.Node;
import io.atomix.cluster.NodeConfig;
import io.atomix.cluster.NodeId;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixConfig;
import io.atomix.core.config.impl.DefaultConfigService;
import io.atomix.messaging.impl.NettyMessagingService;
import io.atomix.rest.ManagedRestService;
import io.atomix.rest.RestService;
import io.atomix.utils.net.Address;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Atomix agent runner.
 */
public class AtomixAgent {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixAgent.class);

  public static void main(String[] args) throws Exception {
    ArgumentType<NodeConfig> nodeArgumentType = (ArgumentParser argumentParser, Argument argument, String value) -> {
      String[] address = parseInfo(value);
      return new NodeConfig()
          .setId(parseNodeId(address))
          .setType(Node.Type.CORE)
          .setAddress(parseAddress(address));
    };

    ArgumentType<Node.Type> typeArgumentType = (ArgumentParser argumentParser, Argument argument, String value) -> Node.Type.valueOf(value.toUpperCase());
    ArgumentType<File> fileArgumentType = (ArgumentParser argumentParser, Argument argument, String value) -> new File(value);

    ArgumentParser parser = ArgumentParsers.newArgumentParser("AtomixServer")
        .defaultHelp(true)
        .description("Atomix server");
    parser.addArgument("node")
        .type(nodeArgumentType)
        .nargs("?")
        .metavar("NAME:HOST:PORT")
        .required(false)
        .help("The local node info");
    parser.addArgument("--type", "-t")
        .type(typeArgumentType)
        .metavar("TYPE")
        .choices("core", "data", "client")
        .setDefault(Node.Type.CORE)
        .help("Indicates the local node type");
    parser.addArgument("--config", "-c")
        .type(fileArgumentType)
        .metavar("FILE")
        .required(false)
        .help("The Atomix configuration file");
    parser.addArgument("--core-nodes", "-n")
        .nargs("*")
        .type(nodeArgumentType)
        .metavar("NAME:HOST:PORT")
        .required(false)
        .help("Sets the core cluster configuration");
    parser.addArgument("--bootstrap-nodes", "-b")
        .nargs("*")
        .type(nodeArgumentType)
        .metavar("NAME:HOST:PORT")
        .required(false)
        .help("Sets the bootstrap nodes");
    parser.addArgument("--data-dir", "-d")
        .type(fileArgumentType)
        .metavar("FILE")
        .required(false)
        .help("The data directory");
    parser.addArgument("--http-port", "-p")
        .type(Integer.class)
        .metavar("PORT")
        .required(false)
        .setDefault(5678)
        .help("An optional HTTP server port");

    Namespace namespace = null;
    try {
      namespace = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    AtomixConfig config;
    File configFile = namespace.get("config");
    if (configFile != null) {
      config = new DefaultConfigService().load(configFile, AtomixConfig.class);
    } else {
      config = new AtomixConfig();
    }

    NodeConfig localNode = namespace.get("node");

    if (localNode != null) {
      Node.Type type = namespace.get("type");
      localNode.setType(type);
      config.getClusterConfig().setLocalNode(localNode);
    }

    List<Node> coreNodes = namespace.getList("core_nodes");
    List<Node> bootstrapNodes = namespace.getList("bootstrap_nodes");

    File dataDir = namespace.get("data_dir");
    Integer httpPort = namespace.getInt("http_port");

    config.setEnableShutdownHook(true)
        .setDataDirectory(dataDir);

    LOGGER.info("node: {}", localNode);
    LOGGER.info("core-nodes: {}", coreNodes);
    LOGGER.info("bootstrap: {}", bootstrapNodes);
    LOGGER.info("data-dir: {}", dataDir);

    Atomix atomix = new Atomix(config);

    atomix.start().join();

    LOGGER.info("Atomix listening at {}:{}", atomix.clusterService().getLocalNode().address().address().getHostAddress(), atomix.clusterService().getLocalNode().address().port());

    ManagedRestService rest = RestService.builder()
        .withAtomix(atomix)
        .withAddress(Address.from(atomix.clusterService().getLocalNode().address().address().getHostAddress(), httpPort))
        .build();

    rest.start().join();

    LOGGER.info("HTTP server listening at {}:{}", atomix.clusterService().getLocalNode().address().address().getHostAddress(), httpPort);

    synchronized (Atomix.class) {
      while (atomix.isRunning()) {
        Atomix.class.wait();
      }
    }
  }

  static String[] parseInfo(String address) {
    String[] parsed = address.split(":");
    if (parsed.length > 3) {
      throw new IllegalArgumentException("Malformed address " + address);
    }
    return parsed;
  }

  static NodeId parseNodeId(String[] address) {
    if (address.length == 3) {
      return NodeId.from(address[0]);
    } else if (address.length == 2) {
      try {
        InetAddress.getByName(address[0]);
      } catch (UnknownHostException e) {
        return NodeId.from(address[0]);
      }
      return NodeId.from(parseAddress(address).address().getHostName());
    } else {
      try {
        InetAddress.getByName(address[0]);
        return NodeId.from(parseAddress(address).address().getHostName());
      } catch (UnknownHostException e) {
        return NodeId.from(address[0]);
      }
    }
  }

  static Address parseAddress(String[] address) {
    String host;
    int port;
    if (address.length == 3) {
      host = address[1];
      port = Integer.parseInt(address[2]);
    } else if (address.length == 2) {
      try {
        host = address[0];
        port = Integer.parseInt(address[1]);
      } catch (NumberFormatException e) {
        host = address[1];
        port = NettyMessagingService.DEFAULT_PORT;
      }
    } else {
      try {
        InetAddress.getByName(address[0]);
        host = address[0];
      } catch (UnknownHostException e) {
        host = "0.0.0.0";
      }
      port = NettyMessagingService.DEFAULT_PORT;
    }
    return Address.from(host, port);
  }
}
