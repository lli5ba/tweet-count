package com.microsoft.example;

import java.text.BreakIterator;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Map.Entry;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.sql.Timestamp;
import java.util.Date;

public class WordCount extends BaseFunction {
	
	//For holding words and counts
    HashMap<String, Integer> counts = new HashMap<String, Integer>();
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
	Date date2 = new java.util.Date();
	double seconds = (date2.getTime()-date.getTime())/1000;
	if (seconds >= 30) {
		this.date = date2;
		//System.out.println(counts.toString());
		System.out.print(new Timestamp(date2.getTime()));
		System.out.print("	");
		printTopTen();
		//clear hashmap
		counts.clear();
        
	}
	
}
// From: http://stackoverflow.com/questions/18971849/best-way-to-get-top-n-keyssorted-by-values-in-a-hashmap
public static List<String> topNKeys(final HashMap<String, Integer> map, int n) {
    PriorityQueue<String> topN = new PriorityQueue<String>(n, new Comparator<String>() {
        public int compare(String s1, String s2) {
            return Integer.compare(map.get(s1), map.get(s2));
        }
    });

    for(String key:map.keySet()){
        if (topN.size() < n)
            topN.add(key);
        else if (map.get(topN.peek()) < map.get(key)) {
            topN.poll();
            topN.add(key);
        }
    }
    return (List) Arrays.asList(topN.toArray());
}
public void printTopTen() {
	//print top 10 words (words with highest counts)
	List<String> top10 = topNKeys(counts, 10);
	for(String s: top10) {
		System.out.print(s + " ");
	}
	System.out.println();
}
}


