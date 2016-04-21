package com.zt.logparser;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogParser  implements Tool{
	public static class Map extends  Mapper<LongWritable,Text,Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fieldStrings=  line.replaceAll("  ", " ").split(" ");
			String resultString = fieldStrings[0] + "," + fieldStrings[1] +  ","  + fieldStrings[2] + "," + fieldStrings[3] + "," + fieldStrings[4]  + "," + fieldStrings[5]  + "," + fieldStrings[6] + "," + fieldStrings[7] +"," + fieldStrings[8]  + "," +  fieldStrings[9] +"," + fieldStrings[11] +"," + fieldStrings[12] +"," + fieldStrings[13] +"," + fieldStrings[17] ;
		
			System.out.println(resultString.toString());
			context.write(new Text(resultString),new Text());
		}
		
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text>{

		protected void reduce(Text key , Text value,
			Context context) throws IOException, InterruptedException{
			context.write(null , value);
		}
	}
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new LogParser(), args)	;
		System.exit(ret);
	}

	public int run(String[] args) throws Exception{
		Job job = Job.getInstance();
        job.setJarByClass(LogParser.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(Map.class);
//        job.setReducerClass(Reduce.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success =  job.waitForCompletion(true);
        
        return success ? 1:0;
	
	
}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}
}
