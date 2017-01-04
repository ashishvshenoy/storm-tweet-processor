package storm.starter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.starter.bolt.FilePrinterBolt;
import org.apache.storm.starter.bolt.FileWriterBolt;
import org.apache.storm.starter.bolt.FriendsCountFilterBolt;
import org.apache.storm.starter.bolt.HashtagBolt;
import org.apache.storm.starter.bolt.PopularWordBolt;
import org.apache.storm.starter.bolt.PopularWordsPrinterBolt;
import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.starter.bolt.StopWordBolt;
import org.apache.storm.starter.bolt.TweetPrinterBolt;
import org.apache.storm.starter.bolt.WordCountBolt;
import org.apache.storm.starter.spout.FriendsCountSpout;
import org.apache.storm.starter.spout.HashtagSpout;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.starter.util.AssignmentArguments;
import org.apache.storm.starter.util.ReadStopWords;
import org.apache.storm.thrift.TException;

public class CS838Assignment2PartC2 {
	public static void main(String[] args) {
		String TOPOLOGY_NAME = AssignmentArguments.TOPOLOGY_NAME_2;
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];
		String fileName = args[4];
		String clusterModeString = args[5];
		String stopWordsPath = args[6];
		String[] arguments = args.clone();
		boolean clusterMode = false;
		if(clusterModeString.equals("true")){
			clusterMode = true;
		} else {
			clusterMode = false;
		}
		
		String[] keyWords = AssignmentArguments.KEYWORDS;
		String[] stopWords = ReadStopWords.read(stopWordsPath);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitterStream",
				new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords, -1)).setNumTasks(4);
		String[] hashtags = AssignmentArguments.HASHTAGS;
		builder.setSpout("hashtagSpout", new HashtagSpout(hashtags));
		Integer[] friendsCount = AssignmentArguments.FRIENDS_COUNT;
		builder.setSpout("friendsCountSpout", new FriendsCountSpout(friendsCount));
		builder.setBolt("friendsCountFilter", new FriendsCountFilterBolt())
				.shuffleGrouping("friendsCountSpout").shuffleGrouping("twitterStream");
		builder.setBolt("hashtagFilter", new HashtagBolt())
				.shuffleGrouping("hashtagSpout").shuffleGrouping("friendsCountFilter");
		builder.setBolt("stopwordFilter", new StopWordBolt(stopWords)).shuffleGrouping("hashtagFilter");
		builder.setBolt("wordCountBolt", new WordCountBolt()).fieldsGrouping("stopwordFilter",new Fields("word"));
		builder.setBolt("popularWordsBolt",new PopularWordBolt()).globalGrouping("wordCountBolt");
		

		if (clusterMode == true) {
			
			// Use pipe as record boundary
			RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

			// Synchronize data buffer with the filesystem every 1000 tuples
			SyncPolicy syncPolicy = new CountSyncPolicy(1000);

			// Rotate data files when they reach five MB
			FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

			// Use default, Storm-generated file names
			FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(fileName+"/popularWords");

			// Instantiate the HdfsBolt
			HdfsBolt bolt = new HdfsBolt().withFsUrl("hdfs://10.254.0.33:8020").withFileNameFormat(fileNameFormat)
					.withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);
			builder.setBolt("printPopularWords", bolt).globalGrouping("popularWordsBolt");
			
			FileNameFormat fileNameFormat2 = new DefaultFileNameFormat().withPath(fileName+"/tweets_hashtag");
			HdfsBolt bolt2 = new HdfsBolt().withFsUrl("hdfs://10.254.0.33:8020").withFileNameFormat(fileNameFormat2)
					.withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);
			builder.setBolt("printTweets", bolt2).globalGrouping("hashtagFilter");

			Config clusterConf = new Config();
			clusterConf.put(Config.TOPOLOGY_WORKERS, 4);
			clusterConf.put(Config.TOPOLOGY_DEBUG, true);
			clusterConf.setMaxSpoutPending(5000);
			
			try {
				StormSubmitter.submitTopology(TOPOLOGY_NAME, clusterConf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Utils.sleep(1000000);
			NimbusClient cc = NimbusClient.getConfiguredClient(clusterConf);

			Nimbus.Client client = cc.getClient();
			try {
				client.killTopology(TOPOLOGY_NAME);
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		else {
			Config conf = new Config();
			conf.put("output_file", fileName);
			LocalCluster cluster = new LocalCluster();
			builder.setBolt("printPopularWords", new PopularWordsPrinterBolt(fileName+"/popularWords.txt")).globalGrouping("popularWordsBolt");
			builder.setBolt("printTweets", new TweetPrinterBolt(fileName+"/tweets_hashtag.txt")).globalGrouping("hashtagFilter");
			cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
			Utils.sleep(1000000);
			cluster.shutdown();
		}

	}

	public static int getCount(String filePath) throws IOException {
		BufferedReader reader;
		int lines = 0;
		try {
			reader = new BufferedReader(new FileReader(filePath));

			while (reader.readLine() != null)
				lines++;
			reader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lines;

	}
}