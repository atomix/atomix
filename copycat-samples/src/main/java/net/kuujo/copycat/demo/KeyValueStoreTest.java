package net.kuujo.copycat.demo;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.impl.DynamicClusterConfig;

public class KeyValueStoreTest {

	public static void main(String[] args) {
		
		StateMachine stateMachine = new KeyValueStore();
		
		ClusterConfig cluster = new DynamicClusterConfig();
		
		CopyCatContext context = new CopyCatContext(stateMachine, cluster);
		context.start();
	}

}
