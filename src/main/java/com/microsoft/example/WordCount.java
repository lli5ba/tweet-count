package com.microsoft.example;

import java.text.BreakIterator;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.HashMap;
import java.util.Map;

import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.sql.Timestamp;
import java.util.Date;

public class WordCount extends BaseFunction {

	//For holding words and counts
    Map<String, Integer> counts = new HashMap<String, Integer>();
    //For holding last date
    Date date = new java.util.Date();
  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    //Get the word contents from the tuple
     String word = tuple.getString(0);
	 //Have we counted any already?
      Integer count = counts.get(word);
	   if (count == null)
        count = 0;
      //Increment the count and store it
      count++;
      counts.put(word, count);
      this.updateDate();
      
      
      //Emit the word and the current count
      collector.emit(new Values(word, count));
} 
public void updateDate(){
	date2 = new java.util.Date();
	double seconds = (date2.getTime()-date.getTime())/1000;
	System.out.println(seconds);
	if (seconds >= 5) {
		System.out.println(new Timestamp(date2.getTime()));
		this.date = date2;
	}
	
}
	//Declare that we will emit a tuple containing two fields; word and count
    //@Override
    //public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //  declarer.declare(new Fields("hashtag", "count"));
    //}
  
}
