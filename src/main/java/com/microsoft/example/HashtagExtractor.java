package com.microsoft.example;

import java.text.BreakIterator;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashtagExtractor extends BaseFunction {
  ArrayList<String> stopwords = loadStopwords("./data/stopwords.txt");
  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
	//Get the tweet
	final Status status = (Status) tuple.get(0);
	String sentence = status.getText();
	System.out.println(sentence);
	//NOTE: The following code is from the Microsoft wordcount tutorial:
	//https://azure.microsoft.com/en-us/documentation/articles/hdinsight-storm-develop-java-topology/
	String[] wordsInTweet = sentence.split(" ");    
	//to lowercase and 
	for ( String word : wordsInTweet) {
		//remove urls, and usertags
		word=word.replaceAll("^https?://[^/]+/([^/]+)/","");
		word=word.replaceAll("@[^\\s]+", "");
		//send to lowercase
		word=word.toLowerCase();
		//remove punctuation
		word=word.replaceAll("[^a-zA-Z ]", "");
	
		if (!word.equals("")) {
			collector.emit(new Values(word));
		}
	}
	
	
	
}
	
}
  
  public ArrayList<String> loadStopwords(String filename) {
  	System.out.println("starting load");
    ArrayList<String> words = new ArrayList<String>();
	try {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"));
		String line;
		
		while ((line = reader.readLine()) != null) {
			if (!line.isEmpty()){
				System.out.println(line);
				words.add(line);
			}
		}
		reader.close();
		
	} catch(IOException e){
		System.err.format("[Error]Failed to open file %s!!", filename);
	}
	return words;
}
	
}
