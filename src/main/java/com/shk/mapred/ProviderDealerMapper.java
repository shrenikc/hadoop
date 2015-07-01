package com.shk.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.logging.log4j.*;

public class ProviderDealerMapper extends Mapper<LongWritable, Text, ProviderDealer, StatsCounterTuple> {

	private static final Logger logger = LogManager.getLogger(ProviderDealerMapper.class);
	
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

        // Since description column can contain spaces or special characters use index from end 
        if(NEW.equals(tokens[tokens.length - 3])) {
        	counter.setNewCounter(1);
        } else if(USED.equals(tokens[tokens.length - 3])) {
        	counter.setUsedCounter(1);
        } else {
        	// Increase counter for Invalid values
        	logger.error("Invalid value of UsedNew for : " + providerDealer);
        	context.getCounter(COUNTERS.INVALID_NEWUSED).increment(1);
        }

        // Since description column can contain spaces or special characters use index from end
        if(SUCCEEDED.equals(tokens[tokens.length - 1])) {
        	counter.setSuccessCounter(1);
        } else if (FAILED.equals(tokens[tokens.length - 1])){
        	counter.setFailedCounter(1);
        } else {
        	// Increase counter for Invalid values
        	logger.error("Invalid value of SucceededFailed for : " + providerDealer);
        	context.getCounter(COUNTERS.INVALID_SUCCEEDEDFAILED).increment(1);
        }

        context.write(providerDealer, counter);
    }
}
