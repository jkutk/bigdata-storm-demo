package at.tk.jku.steinbauer.bigdata.storm.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class Twitter4JSpout extends BaseRichSpout {

	private static final long serialVersionUID = 5053951197840706598L;
	
	private LinkedBlockingQueue<Status> tweetQueue;
	
	private SpoutOutputCollector collector;
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
	
	public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.tweetQueue = new LinkedBlockingQueue<Status>();
		final StatusListener statusListener = new StatusListener() {
			public void onStatus(Status status) {
				try {
					tweetQueue.put(status);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			public void onException(Exception ex) { ex.printStackTrace(); }
			public void onTrackLimitationNotice(int limit) { System.err.println("We hit the Gardenhose limitation " + limit); }
			public void onStallWarning(StallWarning stall) { }
			public void onScrubGeo(long arg0, long arg1) { }
			public void onDeletionNotice(StatusDeletionNotice del) { }
		};
		TwitterStream stream = TwitterStreamFactory.getSingleton();
		stream.addListener(statusListener);
		stream.sample();
	}

	public void nextTuple() {
		try {
			this.collector.emit(new Values(this.tweetQueue.take()));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
