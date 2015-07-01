package com.shk.mapred;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class ProviderDealerReducer extends Reducer<ProviderDealer, StatsCounterTuple, ProviderDealer, StatsCounterTuple>{

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
