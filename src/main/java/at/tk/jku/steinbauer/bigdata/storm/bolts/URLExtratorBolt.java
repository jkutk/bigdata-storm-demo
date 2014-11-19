package at.tk.jku.steinbauer.bigdata.storm.bolts;

import java.util.Map;

import twitter4j.Status;
import twitter4j.URLEntity;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class URLExtratorBolt extends BaseRichBolt {

	private static final long serialVersionUID = -5375319117082718802L;

	private OutputCollector collector;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
	}

	public void prepare(@SuppressWarnings("rawtypes") Map config,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		Status tweet = (Status) tuple.getValueByField("tweet");
		for (URLEntity url : tweet.getURLEntities()) {
			this.collector.emit(new Values(url.getURL()));
		}
	}
}
