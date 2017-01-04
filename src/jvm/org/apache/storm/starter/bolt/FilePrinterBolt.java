package org.apache.storm.starter.bolt;

import twitter4j.Status;

import java.io.IOException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.lang.Iterable;
import java.util.Arrays;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class FilePrinterBolt extends BaseBasicBolt {
    static int numberOfPrint = 0;
    String _fileToWrite;
    OutputStream o;

    public FilePrinterBolt(String _fileToWrite){
    	this._fileToWrite = _fileToWrite;
    }
    
    public void setPrintFile(String FileToPrint){
        _fileToWrite = FileToPrint;
    }

    public int getNumOfPrint(){
        return numberOfPrint;
    }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
	System.out.println("******");
    System.out.println(((Status)tuple.getValue(0)).getText());
    //
    setPrintFile(_fileToWrite);
    Status stat = (Status)tuple.getValue(0);
    String str = stat.getText();
    if( !stat.getLang().equals("en"))
        return;

    try{
        o = new FileOutputStream(_fileToWrite, true);
        o.write(str.getBytes());
        o.close();
    }catch (IOException e){
        e.printStackTrace();
    }

    numberOfPrint++;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}
