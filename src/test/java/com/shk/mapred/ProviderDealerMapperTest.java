package com.shk.mapred;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.shk.mapred.ProviderDealerMapper.COUNTERS;

import static org.junit.Assert.*;


@RunWith(MockitoJUnitRunner.class)
public class ProviderDealerMapperTest {
    private MapDriver<LongWritable, Text, ProviderDealer, StatsCounterTuple> mapDriver;
    private ReduceDriver<ProviderDealer, StatsCounterTuple, ProviderDealer, StatsCounterTuple> reduceDriver;
    private MapReduceDriver<LongWritable, Text, ProviderDealer, StatsCounterTuple, ProviderDealer, StatsCounterTuple> mapReduceDriver;

    @Before
    public void setUp() {
    	
    	ProviderDealerMapper mapper = new ProviderDealerMapper();
    	ProviderDealerReducer reducer = new ProviderDealerReducer();
    	
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException, InterruptedException {
    	
    	mapDriver.withInput(new LongWritable(0), new Text("1 123 1G1RB6E40DU1XXXXX Chevrolet Volt 2013 Some trim Some description Used 29995 Succeeded"));
    	mapDriver.withOutput(new ProviderDealer(new Text("1"), new Text("123")), new StatsCounterTuple(1, 0, 1, 0));
        mapDriver.runTest();
    }
    
    @Test
    public void testReducer() throws IOException, InterruptedException {
    	
    	List<StatsCounterTuple> counterList = new ArrayList<StatsCounterTuple>();
    	counterList.add(new StatsCounterTuple(1, 0, 1, 0));
    	counterList.add(new StatsCounterTuple(0, 1, 0, 1));

    	
    	reduceDriver.withInput(new ProviderDealer(new Text("1"), new Text("123")), counterList);
    	reduceDriver.withOutput(new ProviderDealer(new Text("1"), new Text("123")), new StatsCounterTuple(1, 1, 1, 1));
    	
    	reduceDriver.runTest();
        
    }
    
    @Test
    public void testMapReducer() throws IOException, InterruptedException {
    	mapReduceDriver.withInput(new LongWritable(0), new Text("5 123 1G1RB6E40DU1XXXXX Chevrolet Volt 2013 Some trim Some description Used 29995 Succeeded"));
    	mapReduceDriver.withInput(new LongWritable(1), new Text("5 123 1G1RB6E40DU1XXXXX Chevrolet Volt 2013 Some trim Some description New 29995 Failed"));
    	mapReduceDriver.withInput(new LongWritable(2), new Text("5 123 1G1RB6E40DU1XXXXX Chevrolet Volt 2013 Some trim Some description Used 29995 Succeeded"));
        
    	mapReduceDriver.withOutput(new ProviderDealer(new Text("5"), new Text("123")), new StatsCounterTuple(2, 1, 2, 1));
        
    	mapReduceDriver.runTest();
    }
    
    @Test
    public void testCounters()  throws IOException, InterruptedException {
    	mapDriver.withInput(new LongWritable(0), new Text("1 123 1G1RB6E40DU1XXXXX Chevrolet Volt 2013 Some trim Some description UNKNOWN 29995 UNKNOWN"));
    	mapDriver.withOutput(new ProviderDealer(new Text("1"), new Text("123")), new StatsCounterTuple(0, 0, 0, 0));    	
        mapDriver.runTest();
        
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(COUNTERS.INVALID_NEWUSED).getValue());
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(COUNTERS.INVALID_SUCCEEDEDFAILED).getValue());
    }

}
