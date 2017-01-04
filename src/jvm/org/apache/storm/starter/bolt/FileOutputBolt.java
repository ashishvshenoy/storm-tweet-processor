package org.apache.storm.starter.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import twitter4j.Status;

public class FileOutputBolt extends BaseRichBolt {
	OutputCollector _collector;
	PrintWriter out;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		try {
				Path path = Paths.get(String.valueOf(tuple.getValue(0)));
				if(Files.exists(path))
				{
					out = new PrintWriter(new BufferedWriter(new FileWriter(String.valueOf(tuple.getValue(0)),true)));
				}
				else
				{
					out = new PrintWriter(new BufferedWriter(new FileWriter(String.valueOf(tuple.getValue(0)))));
				}
				if(out != null)
				{
					out.println(((Status)tuple.getValue(1)).getText().replace('\n', ' '));
					out.close();
				}
		} catch (IOException e) {
				System.err.println(e);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void cleanup() {

	}

}
