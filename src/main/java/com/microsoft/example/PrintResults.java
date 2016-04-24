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
		if (count == 0) {
			java.util.Date date= new java.util.Date();
			System.out.print(new Timestamp(date.getTime()));
		} else if (count < 10) {
			System.out.printl( " " + tuple.toString());
			count++;
		} else if (count == 10) {
			//reset count
			count = 10;
		}
		return true;
    }
}
