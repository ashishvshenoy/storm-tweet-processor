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
package org.apache.storm.starter.bolt;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import twitter4j.Status;


public class FileWriterBolt extends BaseBasicBolt {
	

  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static String _fileToWrite;
	public static int maxTweets;
	
	public FileWriterBolt(String _fileToWrite){
		this._fileToWrite = _fileToWrite;
	}

@Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    System.out.println(tuple);
    writeStringToFile(_fileToWrite,((Status)tuple.getValue(0)).getText().replace('\n', ' '));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }
  
  public void writeStringToFile(String filePath, String outputString) {
	  try{
		  System.out.println("***"+outputString);
		  FileWriter fw = new FileWriter(filePath, true);
		  fw.write(outputString);
		  fw.write("\n");
		  //lineCount++;
		  fw.close();
	  }catch (IOException e){
		  System.err.println("IOException"+e.getLocalizedMessage());
	  }
  }

}
