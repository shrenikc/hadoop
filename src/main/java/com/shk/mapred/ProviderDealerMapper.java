package com.shk.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class ProviderDealerMapper extends Mapper<LongWritable, Text, ProviderDealer, StatsCounterTuple> {

    private static final String NEW = "New";
    private static final String USED = "Used";
    private static final String SUCCEEDED = "Succeeded";
    private static final String FAILED = "Failed";

    public static enum COUNTERS {
    	  INVALID_NEWUSED,
    	  INVALID_SUCCEEDEDFAILED
    };
    	
    protected void map(LongWritable offset, Text value, Context context) 
            throws IOException, InterruptedException {
        
    	String[] tokens = value.toString().split(" ");
    	
    	
        ProviderDealer providerDealer = new ProviderDealer(new Text(tokens[0]), new Text(tokens[1]));
        StatsCounterTuple counter = new StatsCounterTuple();

        if(NEW.equals(tokens[tokens.length - 3])) {
        	counter.setNewCounter(1);
        } else if(USED.equals(tokens[tokens.length - 3])) {
        	counter.setUsedCounter(1);
        } else {
        	context.getCounter(COUNTERS.INVALID_NEWUSED).increment(1);
        }

        if(SUCCEEDED.equals(tokens[tokens.length - 1])) {
        	counter.setSuccessCounter(1);
        } else if (FAILED.equals(tokens[tokens.length - 1])){
        	counter.setFailedCounter(1);
        } else {
        	context.getCounter(COUNTERS.INVALID_SUCCEEDEDFAILED).increment(1);
        }

        context.write(providerDealer, counter);
    }
}
