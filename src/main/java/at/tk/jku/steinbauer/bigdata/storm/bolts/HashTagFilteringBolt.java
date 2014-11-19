package at.tk.jku.steinbauer.bigdata.storm.bolts;

import java.util.Map;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashTagFilteringBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2661146533959086060L;
	
	private OutputCollector collector;
	
	private String hashtag;
	
	public HashTagFilteringBolt(String hashtag) {
		this.hashtag = hashtag;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		Status tweet = (Status) tuple.getValueByField("tweet");
		for(HashtagEntity hashTag : tweet.getHashtagEntities()) {
			if(hashTag.getText().contains(this.hashtag)) {
				collector.emit(new Values(tweet));
				return;
			}
		}
	}

}
