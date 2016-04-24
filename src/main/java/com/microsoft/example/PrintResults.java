package com.microsoft.example;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import java.sql.Timestamp;
import java.util.Date;

public class PrintResults extends BaseFilter {
	private int count;
    public PrintResults() {
        count = 0;
    }


    @Override
    public boolean isKeep(TridentTuple tuple) {
    		if (count == 10) {
			//go to next line and reset count
			System.out.println();
			count = 0;
		}
		
		if (count < 10) {
			if (count == 0) {
				java.util.Date date= new java.util.Date();
				System.out.print(new Timestamp(date.getTime()));
			}
			//append each word
			System.out.print( " " + tuple.toString());
			count++;
		}
		return true;
    }
}
