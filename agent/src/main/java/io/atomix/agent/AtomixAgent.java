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
import io.atomix.rest.ManagedRestService;
import io.atomix.rest.RestService;
import io.atomix.utils.net.Address;
import io.atomix.utils.net.MalformedAddressException;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.action.StoreTrueArgumentAction;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Atomix agent runner.
 */
public class AtomixAgent {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixAgent.class);

  public static void main(String[] args) throws Exception {
    Function<Node.Type, ArgumentType<NodeConfig>> nodeArgumentType = (type) -> (ArgumentParser argumentParser, Argument argument, String value) -> {
      return new NodeConfig()
          .setId(parseNodeId(value))
          .setType(type)
          .setAddress(parseAddress(value));
    };

    ArgumentType<Node.Type> typeArgumentType = (argumentParser, argument, value) -> Node.Type.valueOf(value.toUpperCase());
    ArgumentType<Address> addressArgumentType = (argumentParser, argument, value) -> Address.from(value);
    ArgumentType<File> fileArgumentType = (argumentParser, argument, value) -> new File(value);

    ArgumentParser parser = ArgumentParsers.newArgumentParser("AtomixServer")
        .defaultHelp(true)
        .description("Atomix server");
    parser.addArgument("node")
        .type(nodeArgumentType.apply(Node.Type.PERSISTENT))
        .nargs("?")
        .metavar("NAME@HOST:PORT")
        .required(false)
        .help("The node info for the local node. This should be in the format [NAME@]HOST[:PORT]. " +
            "If no name is provided, the node name will default to the host. " +
            "If no port is provided, the port will default to 5679.");
    parser.addArgument("--type", "-t")
        .type(typeArgumentType)
        .metavar("TYPE")
        .choices(Node.Type.PERSISTENT, Node.Type.EPHEMERAL, Node.Type.CLIENT)
        .setDefault(Node.Type.PERSISTENT)
        .help("Indicates the local node type.");
    parser.addArgument("--config", "-c")
        .metavar("FILE|JSON|YAML")
        .required(false)
        .help("The Atomix configuration. Can be specified as a file path or JSON/YAML string.");
    parser.addArgument("--core-nodes", "-n")
        .nargs("*")
        .type(nodeArgumentType.apply(Node.Type.PERSISTENT))
        .metavar("NAME@HOST:PORT")
        .required(false)
        .help("The set of core nodes, if any. When bootstrapping a new cluster, if the local node is a core node " +
            "then it should be present in the core configuration as well.");
    parser.addArgument("--bootstrap-nodes", "-b")
        .nargs("*")
        .type(nodeArgumentType.apply(Node.Type.EPHEMERAL))
        .metavar("NAME@HOST:PORT")
        .required(false)
        .help("The set of bootstrap nodes. If core nodes are provided, the cluster will be bootstrapped from the " +
            "core nodes. For clusters without core nodes, at least one bootstrap node must be provided unless " +
            "using multicast discovery or bootstrapping a new cluster.");
    parser.addArgument("--multicast", "-m")
        .action(new StoreTrueArgumentAction())
        .setDefault(false)
        .help("Enables multicast discovery. Note that the network must support multicast for this feature to work.");
    parser.addArgument("--multicast-address", "-a")
        .type(addressArgumentType)
        .metavar("HOST:PORT")
        .help("Sets the multicast discovery address. Defaults to 230.0.0.1:54321");
    parser.addArgument("--data-dir", "-d")
        .type(fileArgumentType)
        .metavar("FILE")
        .required(false)
        .help("Sets the global Atomix data directory used for storing system data.");
    parser.addArgument("--http-port", "-p")
        .type(Integer.class)
        .metavar("PORT")
        .required(false)
        .setDefault(5678)
        .help("Sets the port on which to run the HTTP server. Defaults to 5678");

    Namespace namespace = null;
    try {
      namespace = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    final String configString = namespace.get("config");
    final NodeConfig localNode = namespace.get("node");
    final Node.Type localNodeType = namespace.get("type");
    final List<NodeConfig> coreNodes = namespace.getList("core_nodes");
    final List<NodeConfig> bootstrapNodes = namespace.getList("bootstrap_nodes");
    final boolean multicastEnabled = namespace.getBoolean("multicast");
    final Address multicastAddress = namespace.get("multicast_address");
    final File dataDir = namespace.get("data_dir");
    final Integer httpPort = namespace.getInt("http_port");

    // If a configuration was provided, merge the configuration's node information with the provided command line arguments.
    final Atomix.Builder builder;
    if (configString != null) {
      AtomixConfig config = loadConfig(configString);
      if (localNode != null) {
        config.getClusterConfig().getNodes().stream()
            .filter(node -> node.getId().equals(localNode.getId()))
            .findAny()
            .ifPresent(localNodeConfig -> {
              if (localNodeType == null) {
                localNode.setType(localNodeConfig.getType());
              }
              localNode.setAddress(localNodeConfig.getAddress());
              localNode.setZone(localNodeConfig.getZone());
              localNode.setRack(localNodeConfig.getRack());
              localNode.setHost(localNodeConfig.getHost());
            });
      }
      builder = Atomix.builder(config);
    } else {
      builder = Atomix.builder();
    }

    builder.withShutdownHook(true);

    // If a local node was provided, add the local node to the builder.
    if (localNode != null) {
      localNode.setType(localNodeType);
      Node node = new Node(localNode);
      builder.withLocalNode(node);
      LOGGER.info("node: {}", node);
    }

    // If a cluster configuration was provided, add all the cluster nodes to the builder.
    if (coreNodes != null || bootstrapNodes != null) {
      List<Node> nodes = Stream.concat(
          coreNodes != null ? coreNodes.stream() : Stream.empty(),
          bootstrapNodes != null ? bootstrapNodes.stream() : Stream.empty())
          .map(node -> Node.builder(node.getId())
              .withType(node.getType())
              .withAddress(node.getAddress())
              .build())
          .collect(Collectors.toList());
      builder.withNodes(nodes);
    }

    // Enable multicast if provided.
    if (multicastEnabled) {
      builder.withMulticastEnabled();
      if (multicastAddress != null) {
        builder.withMulticastAddress(multicastAddress);
      }
    }

    if (dataDir != null) {
      builder.withDataDirectory(dataDir);
    }

    Atomix atomix = builder.build();

    atomix.start().join();

    LOGGER.info("Atomix listening at {}:{}", atomix.clusterService().getLocalNode().address().host(), atomix.clusterService().getLocalNode().address().port());

    ManagedRestService rest = RestService.builder()
        .withAtomix(atomix)
        .withAddress(Address.from(atomix.clusterService().getLocalNode().address().host(), httpPort))
        .build();

    rest.start().join();

    LOGGER.info("HTTP server listening at {}:{}", atomix.clusterService().getLocalNode().address().address().getHostAddress(), httpPort);

    synchronized (Atomix.class) {
      while (atomix.isRunning()) {
        Atomix.class.wait();
      }
    }
  }

  /**
   * Loads a configuration from the given file.
   */
  private static AtomixConfig loadConfig(String config) {
    File configFile = new File(config);
    if (configFile.exists()) {
      return new DefaultConfigService().load(configFile, AtomixConfig.class);
    } else {
      return new DefaultConfigService().load(config, AtomixConfig.class);
    }
  }

  static NodeId parseNodeId(String address) {
    int endIndex = address.indexOf('@');
    if (endIndex > 0) {
      return NodeId.from(address.substring(0, endIndex));
    } else {
      try {
        return NodeId.from(Address.from(address).host());
      } catch (MalformedAddressException e) {
        return NodeId.from(address);
      }
    }
  }

  static Address parseAddress(String address) {
    int startIndex = address.indexOf('@');
    if (startIndex == -1) {
      try {
        return Address.from(address);
      } catch (MalformedAddressException e) {
        return Address.from("0.0.0.0");
      }
    } else {
      return Address.from(address.substring(startIndex + 1));
    }
  }
}
