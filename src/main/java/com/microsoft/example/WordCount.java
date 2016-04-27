package com.microsoft.example;

import java.text.BreakIterator;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class WordCount extends BaseFunction {

	//For holding words and counts
    Map<String, Integer> counts = new HashMap<String, Integer>();
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
      //Emit the word and the current count
      collector.emit(new Values(word, count));
    
	//Declare that we will emit a tuple containing two fields; word and count
} 
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("hashtag", "count"));
    }
  
}
