package com.microsoft.example;

import java.text.BreakIterator;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.sql.Timestamp;
import java.util.Date;

public class WordCount extends BaseFunction {

	//For holding words and counts
    Map<String, Integer> counts = new HashMap<String, Integer>();
    //For holding last date
    Date date = new java.util.Date();
    //to sort TreeMap
    ValueComparator bvc =  new ValueComparator(counts);
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
	Date date2 = new java.util.Date();
	double seconds = (date2.getTime()-date.getTime())/1000;
	if (seconds >= 5) {
		this.date = date2;
		System.out.println(counts.toString());
		System.out.print(new Timestamp(date2.getTime()));
		System.out.print(": ");
		printTopTen();
		
        
	}
	
}

public void printTopTen() {
	//print top 10 words (words with highest counts)
	TreeMap<String, Integer> sorted_counts = new TreeMap<String,Integer>(bvc);
	sorted_counts.putAll(counts);
	int i = 0;
	for (String s : sorted_counts.keySet()) {
		if (i == 10) {
			break;
		}
		System.out.println(s + " ");
		i++;
	}
	System.out.println();
}
}

class ValueComparator implements Comparator<String> {

    Map<String, Integer> base;
    public ValueComparator(Map<String, Integer> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
