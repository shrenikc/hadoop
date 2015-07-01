package com.shk.mapred;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProviderDealerReducer extends Reducer<ProviderDealer, StatsCounterTuple, ProviderDealer, StatsCounterTuple>{

	private static final Logger logger = LogManager.getLogger(ProviderDealerReducer.class);
	
	@Override
	protected void reduce(ProviderDealer key,
			Iterable<StatsCounterTuple> value,
			Context context)
			throws IOException, InterruptedException {
		
		StatsCounterTuple finalCounter = new StatsCounterTuple();
		
		for(StatsCounterTuple counter : value) {
			finalCounter.add(counter);
		}

		context.write(key, finalCounter);
	}
}
