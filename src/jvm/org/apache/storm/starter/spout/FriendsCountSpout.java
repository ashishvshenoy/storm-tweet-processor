package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.starter.util.AssignmentArguments;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class FriendsCountSpout extends BaseRichSpout{
	List<Integer> friendsCount;
	SpoutOutputCollector _collector;
	private int sampleSize;
	

	public FriendsCountSpout(Integer[] friendsCount) {
		super();
		this.friendsCount = Arrays.asList(friendsCount);
		this.sampleSize = AssignmentArguments.FRIENDS_SAMPLE_COUNT;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		_collector.emit(new Values(new Date().getTime(), getRandomSampleOfFriendCount()));
		Utils.sleep(AssignmentArguments.INTERVAL);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		List<String> fieldList = new ArrayList<String>();
		fieldList.add("timestamp");
		fieldList.add("friendsCount");
		declarer.declare(new Fields(fieldList));

	}
	
	public List<Integer> getRandomSampleOfFriendCount(){
		Random rand = new Random(new Date().getTime());
		int samplePosition = rand.nextInt(friendsCount.size());
		List<Integer> randomSampleOfFriendsCount = new ArrayList<Integer>();
		int count = friendsCount.get(samplePosition);
		randomSampleOfFriendsCount.add(count);
		return randomSampleOfFriendsCount;
	}
}
