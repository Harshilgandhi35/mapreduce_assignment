import java.io.*;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;





public class CountingTaxi {

	public static class MapeszClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
		 private final static IntWritable one = new IntWritable(1);
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            
	        	 String[] str = value.toString().split(",");
	        	 
	        	 String payment_type = str[9]; 
	        	 
	        	
	        	 context.write(new Text(payment_type),one);
	        		 
	        	 
	        	  
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	public static class ReducegClass extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable val : values)
			{
				sum = sum + val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	   }
	
	 public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(CountingTaxi.class);
		    job.setMapperClass(MapeszClass.class);
		    job.setReducerClass(ReducegClass.class);
		    job.setNumReduceTasks(1);
		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}

