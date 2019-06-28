import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class Graph{
	//private static final String OUTPUT_PATH = "intermediate_output";
	public static void main ( String[] args ) throws Exception {
  
		 Configuration conf = new Configuration();
     
		 Job job = Job.getInstance(conf, "My Job1");
	        job.setJarByClass(Graph.class);
	        job.setOutputKeyClass(LongWritable.class);
	        job.setOutputValueClass(LongWritable.class);
	        job.setMapOutputKeyClass(LongWritable.class);
	        job.setMapOutputValueClass(LongWritable.class);
	        job.setMapperClass(Map1.class);
	        job.setReducerClass(Reduce1.class);
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	        FileInputFormat.setInputPaths(job,new Path(args[0]));
	        FileOutputFormat.setOutputPath(job,new Path("Intermediate_Output"));
	        job.waitForCompletion(true);
          
	       Job job1 = Job.getInstance(conf, "My Job2");
	        job1.setJarByClass(Graph.class);
	        job1.setOutputKeyClass(LongWritable.class);
	        job1.setOutputValueClass(LongWritable.class);
	        job1.setMapOutputKeyClass(LongWritable.class);
	        job1.setMapOutputValueClass(LongWritable.class);
	        job1.setMapperClass(Map2.class);
	        job1.setReducerClass(Reduce2.class);
	        job1.setInputFormatClass( SequenceFileInputFormat.class);
	        job1.setOutputFormatClass(TextOutputFormat.class);
	        FileInputFormat.setInputPaths(job1,new Path("Intermediate_Output"));
	        FileOutputFormat.setOutputPath(job1,new Path(args[1]));
	        job1.waitForCompletion(true);
	        FileUtil.fullyDelete(new File("Intermediate_Output"));
	       
   }
   
	public static class Map1 extends Mapper<Object,Text,LongWritable,LongWritable>{
		@Override
		public void map(Object key, Text value, Context con) throws IOException, InterruptedException
		{
			 Scanner s = new Scanner(value.toString()).useDelimiter(",");
	            int k = s.nextInt();
	            long v = s.nextLong();
	            con.write(new LongWritable(k),new LongWritable(v));
	            s.close();
		}
		}	
	
	public static class Reduce1 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException
		{
		long count = 0;
		   for(LongWritable value : values)
		   {
		   count++;
		   }
		   con.write(key, new LongWritable(count));
		}
		}
    
	public static class Map2 extends Mapper<LongWritable,LongWritable,LongWritable,LongWritable>{
		@Override
		public void map(LongWritable key, LongWritable value, Context con) throws IOException, InterruptedException
		{
	            con.write(value,key);
		}
		}	
	
	public static class Reduce2 extends Reducer<LongWritable,LongWritable,Text,Text>{
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException
		{
		   System.out.println("In Reduce2");
		   long count = 0;
		   for(LongWritable value : values)
		   {
		   count++;
		   }
		   con.write(new Text(key.toString()) , new Text(Long.toString(count)));
		}
		}

}
