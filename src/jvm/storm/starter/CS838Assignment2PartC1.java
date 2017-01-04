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
import org.apache.storm.starter.bolt.FilePrinterBolt;
import org.apache.storm.starter.bolt.FileWriterBolt;
import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.starter.bolt.TweetCountBolt;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.starter.util.AssignmentArguments;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

public class CS838Assignment2PartC1 {        
    public static void main(String[] args) {
        String consumerKey = args[0]; 
        String consumerSecret = args[1]; 
        String accessToken = args[2]; 
        String accessTokenSecret = args[3];
        String clusterModeString = args[4];
        String outputFilePath = args[5];
        boolean clusterMode;
        int maxTweets = 500000;
        if(clusterModeString.equals("true")){
        	clusterMode = true;
        } else {
        	clusterMode = false;
        }
        String[] arguments = args.clone();
        //String[] keyWords = Arrays.copyOfRange(arguments, 5, arguments.length);
        String[] keyWords = {"RT","india","bangalore","instagram","youtube","twitter","flickr","tbt","fml","wtf","elections","clinton","trump","obama","modi","mobile","widget","facebook","search","taylor swift","bieber"};
        
        TopologyBuilder builder = new TopologyBuilder();
        

     // Use pipe as record boundary
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

        //Synchronize data buffer with the filesystem every 1000 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // Rotate data files when they reach five MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

        // Use default, Storm-generated file names
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(outputFilePath);
                

        
        if(clusterMode == true) {
            // Instantiate the HdfsBolt
            HdfsBolt bolt = new HdfsBolt()
                 .withFsUrl("hdfs://10.254.0.33:8020")
                 .withFileNameFormat(fileNameFormat)
                 .withRecordFormat(format)
                 .withRotationPolicy(rotationPolicy)
                 .withSyncPolicy(syncPolicy);
            builder.setBolt("HDFSPrint", bolt)
                    .shuffleGrouping("Counter");
        	Config clusterConf = new Config();
        	clusterConf.setNumWorkers(20);
        	clusterConf.setMaxSpoutPending(5000);
            TwitterSampleSpout twitterSpout = new TwitterSampleSpout(consumerKey, consumerSecret,
                    accessToken, accessTokenSecret, keyWords, maxTweets);
            
            builder.setSpout("twitterStream", twitterSpout);
            builder.setBolt("Counter", new TweetCountBolt(maxTweets)).globalGrouping("twitterStream");
        	try {
				StormSubmitter.submitTopology(AssignmentArguments.TOPOLOGY_NAME_1, clusterConf, builder.createTopology());
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
            /*while(twitterSpout.countOfTweets<maxTweets){
            	//do nothing
            }
            
            NimbusClient cc = NimbusClient.getConfiguredClient(clusterConf);

            Nimbus.Client client = cc.getClient();

            try {
				client.killTopology(AssignmentArguments.TOPOLOGY_NAME_1);
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
        }
        
        else{
            
			Config conf = new Config();
			conf.put("output_file", outputFilePath);
			LocalCluster cluster = new LocalCluster();
	        TwitterSampleSpout twitterSpout = new TwitterSampleSpout(consumerKey, consumerSecret,
	                accessToken, accessTokenSecret, keyWords, -1);
	        
	        builder.setSpout("twitterStream", twitterSpout);
			builder.setBolt("print", new FileWriterBolt(outputFilePath)).globalGrouping("twitterStream");
			cluster.submitTopology(AssignmentArguments.TOPOLOGY_NAME_1, conf, builder.createTopology());
			Utils.sleep(1000000);
			cluster.shutdown();
        }
        

    }
    
    public static int getCount(String filePath) throws IOException{
    	BufferedReader reader;
    	int lines = 0;
		try {
			reader = new BufferedReader(new FileReader(filePath));

	    	while (reader.readLine() != null) lines++;
	    	reader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return lines;

    }
}
