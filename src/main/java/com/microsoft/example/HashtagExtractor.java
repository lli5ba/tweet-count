package com.microsoft.example;

import java.text.BreakIterator;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashtagExtractor extends BaseFunction {

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    //Get the tweet
    final Status status = (Status) tuple.get(0);
    sentence = status.getText();
    //NOTE: The following code is from the Microsoft wordcount tutorial:
    //https://azure.microsoft.com/en-us/documentation/articles/hdinsight-storm-develop-java-topology/
    //An iterator to get each word
    BreakIterator boundary=BreakIterator.getWordInstance();
    //Give the iterator the sentence
    boundary.setText(sentence);
    //Find the beginning first word
    int start=boundary.first();
    //Iterate over each word and emit it to the output stream
    for (int end=boundary.next(); end != BreakIterator.DONE; start=end, end=boundary.next()) {
      //get the word
      String word=sentence.substring(start,end);
      //If a word is whitespace characters, replace it with empty
      word=word.replaceAll("\\s+","");
      //if it's an actual word, emit it
      if (!word.equals("")) {
        collector.emit(new Values(word));
      }
    }
    
  }
}
