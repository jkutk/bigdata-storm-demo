package at.tk.jku.steinbauer.bigdata.storm.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class URLGroupingBolt extends BaseRichBolt {

	private static final long serialVersionUID = -4397543703782626806L;

	private OutputCollector collector;

	private HashMap<String, Long> counter;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url", "occ"));
	}

	public void prepare(@SuppressWarnings("rawtypes") Map config,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counter = new HashMap<String, Long>();
	}

	public void execute(Tuple tuple) {
		String url = tuple.getStringByField("url");
		Long value = this.counter.get(url);
		if (value == null) {
			value = 0L;
		}
		value++;
		this.counter.put(url, value);
		this.collector.emit(new Values(url, value));
	}

}
