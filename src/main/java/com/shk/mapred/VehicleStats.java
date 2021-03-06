package com.shk.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class VehicleStats extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new VehicleStats(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: hadoop jar hadoop-example-0.0.1-SNAPSHOT-job.jar"
                                       + " [generic options] <in> <out>");
            System.out.println();
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }
        
        Job job = new Job(getConf(), "VehicleStats");
        job.setJarByClass(getClass());
        
        job.setMapperClass(ProviderDealerMapper.class);
        job.setCombinerClass(ProviderDealerReducer.class);
        job.setReducerClass(ProviderDealerReducer.class);
        
        job.setOutputKeyClass(ProviderDealer.class);
        job.setOutputValueClass(StatsCounterTuple.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        boolean success = job.waitForCompletion(true);
        
        return success ? 0 : 1;
    }
}
