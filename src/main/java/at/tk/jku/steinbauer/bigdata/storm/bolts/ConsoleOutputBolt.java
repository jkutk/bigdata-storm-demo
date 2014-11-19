package at.tk.jku.steinbauer.bigdata.storm.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ConsoleOutputBolt extends BaseRichBolt {

	private static final long serialVersionUID = 6577727125344709086L;

	public void declareOutputFields(OutputFieldsDeclarer declarer) { }
	public void prepare(@SuppressWarnings("rawtypes") Map config, TopologyContext context, OutputCollector collector) { }
	
	public void execute(Tuple tuple) {
		StringBuffer buffer = new StringBuffer();
		for(Object o : tuple.getValues()) {
			if(buffer.length() > 0) {
				buffer.append("\t");
			}
			buffer.append(o.toString());
		}
		System.out.println(buffer.toString());
	}



}
