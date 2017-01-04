/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.starter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

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
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.starter.bolt.FileOutputBolt;
import org.apache.storm.starter.bolt.FileWriterBolt;
import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.starter.util.AssignmentArguments;
import org.apache.storm.thrift.TException;

public class PrintSampleStream {        
    public static void main(String[] args) {
        String consumerKey = args[0]; 
        String consumerSecret = args[1]; 
        String accessToken = args[2]; 
        String accessTokenSecret = args[3];
        String fileName = args[4];
        String[] arguments = args.clone();
        boolean clusterMode = true;
        //String[] keyWords = Arrays.copyOfRange(arguments, 5, arguments.length);
        String[] keyWords = {"halloween","india","hockey","ashish","october","hello","happy","amazon","pak","ekta"};
        
        TopologyBuilder builder = new TopologyBuilder();
        

     // Use pipe as record boundary
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

        //Synchronize data buffer with the filesystem every 1000 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // Rotate data files when they reach five MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

        // Use default, Storm-generated file names
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/ubuntu/foo");

        // Instantiate the HdfsBolt
        HdfsBolt bolt = new HdfsBolt()
             .withFsUrl("hdfs://10.254.0.33:8020")
             .withFileNameFormat(fileNameFormat)
             .withRecordFormat(format)
             .withRotationPolicy(rotationPolicy)
             .withSyncPolicy(syncPolicy);
        builder.setBolt("print", bolt)
                .shuffleGrouping("twitter");
                
                
        Config conf = new Config();
        conf.put("output_file", fileName);
        builder.setSpout("twitter", new TwitterSampleSpout(consumerKey, consumerSecret,
                accessToken, accessTokenSecret, keyWords, 500000));
        
        if(clusterMode == true) {
        	Config clusterConf = new Config();
        	clusterConf.setNumWorkers(20);
        	clusterConf.setMaxSpoutPending(5000);
        	try {
				StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
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
            NimbusClient cc = NimbusClient.getConfiguredClient(conf);

            Nimbus.Client client = cc.getClient();
            try {
				client.killTopology("mytopology");
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
        else{
        	LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology("test", conf, builder.createTopology());
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
