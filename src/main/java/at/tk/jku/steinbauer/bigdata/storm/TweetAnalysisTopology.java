package at.tk.jku.steinbauer.bigdata.storm;

import at.tk.jku.steinbauer.bigdata.storm.bolts.ConsoleOutputBolt;
import at.tk.jku.steinbauer.bigdata.storm.bolts.HashTagFilteringBolt;
import at.tk.jku.steinbauer.bigdata.storm.bolts.URLExtratorBolt;
import at.tk.jku.steinbauer.bigdata.storm.bolts.URLGroupingBolt;
import at.tk.jku.steinbauer.bigdata.storm.spouts.Twitter4JSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Streams all tweets with a certain hashtag of off Twitter and tries to perform
 * sentiment analysis on top of them 
 * 
 * @author matthias
 */
public class TweetAnalysisTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
		TopologyBuilder topoBuilder = new TopologyBuilder();
		
		Config config = new Config();
		config.setDebug(false);
		
		if(args != null && args.length == 2) {
			String host = args[0];
			String hashtag = args[1];
			
			topoBuilder.setSpout("twitter-spout", new Twitter4JSpout());
			topoBuilder.setBolt("filter", new HashTagFilteringBolt(hashtag)).shuffleGrouping("twitter-spout");
			topoBuilder.setBolt("url", new URLExtratorBolt()).shuffleGrouping("filter");
			topoBuilder.setBolt("count", new URLGroupingBolt()).fieldsGrouping("url", new Fields("url"));
			topoBuilder.setBolt("output", new ConsoleOutputBolt()).shuffleGrouping("count");
			
			if("local".equalsIgnoreCase(host)) {
				config.setMaxTaskParallelism(3);
				LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology("twitter-sentiment", config, topoBuilder.createTopology());
				Thread.sleep(1000 * 60 * 60); // one hour
				localCluster.shutdown();
			}else{
				config.setNumWorkers(10);
				StormSubmitter.submitTopology("twitter-sentiment", config, topoBuilder.createTopology());
			}
		}

	}

}
