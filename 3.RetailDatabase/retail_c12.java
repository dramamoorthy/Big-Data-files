import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;






public class retail_c12 {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
		//private Text str1 = new Text();
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");
	            //String str1 ="my_key";
	            //long vol = Long.parseLong(str[8]);
	            long sales = Long.parseLong(str[8]);
	            long cost = Long.parseLong(str[7]);
	            long viable = sales-cost;
	            
	            String str2 = str[2]+","+viable;
	            context.write(new Text(str[5]),new Text(str2));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	
	public static class CaderPartitioner extends Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str6 = value.toString().split(",");
	         //int age = Integer.parseInt(str6[2]);


	         if(str6[0].contains("A"))
	         {
	            return 0 % numReduceTasks;
	         }
	         else if(str6[0].contains("B"))
	         {
	            return 1 % numReduceTasks ;
	         }
	         else if(str6[0].contains("C"))
	         {
	            return 2 % numReduceTasks;
	         }
	         else if(str6[0].contains("D"))
	         {
	            return 3 % numReduceTasks;
	         }
	         else if(str6[0].contains("E"))
	         {
	            return 4 % numReduceTasks;
	         }
	         else if(str6[0].contains("F"))
	         {
	            return 5 % numReduceTasks;
	         }
	         else if(str6[0].contains("G"))
	         {
	            return 6 % numReduceTasks;
	         }
	         else if(str6[0].contains("H"))
	         {
	            return 7 % numReduceTasks;
	         }
	         else if(str6[0].contains("I"))
	         {
	            return 8 % numReduceTasks;
	         }
	         else if(str6[0].contains("J"))
	         {
	            return 9 % numReduceTasks;
	         }
	         else 
	         {
	            return 10 % numReduceTasks;
	         }
	      }
	   }
	  public static class ReduceClass extends Reducer<Text,Text,LongWritable,Text>
	   {
		    private LongWritable result = new LongWritable();
		    private Text result1 = new Text();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      long sum = 0;
		    
		      String st= "";
		         for ( Text val : values)
		        	
		         {   
		        	 String[] str3=val.toString().split(",");
		        	 LongWritable value = new LongWritable(Integer.parseInt(str3[1]));
		        	 
		        	 sum += value.get(); 
		        	 st=key+"\t"+str3[0];
		 			}     
		         
		      
		      
		      
		      result.set(sum);
		      result1.set(st);
		      
		      context.write(result,result1);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  
	  public static class SortMapper extends Mapper<LongWritable,Text,DoubleWritable,Text>
		{   private Text result2 = new Text();
		    String st1="";
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
				String[] valueArr = value.toString().split("\t");
				st1=valueArr[1]+"\t"+valueArr[2];
				result2.set(st1);
				context.write(new DoubleWritable(Double.parseDouble(valueArr[0])), new Text(result2));
			}
		}

	  
	  public static class CaderPartitioner1 extends Partitioner < DoubleWritable, Text >
	   {
	      @Override
	      public int getPartition(DoubleWritable key, Text value, int numReduceTasks)
	      {
	         String[] str6 = value.toString().split("\t");
	         //int age = Integer.parseInt(str6[2]);


	         if(str6[1].contains("A"))
	         {
	            return 0 % numReduceTasks;
	         }
	         else if(str6[1].contains("B"))
	         {
	            return 1 % numReduceTasks ;
	         }
	         else if(str6[1].contains("C"))
	         {
	            return 2 % numReduceTasks;
	         }
	         else if(str6[1].contains("D"))
	         {
	            return 3 % numReduceTasks;
	         }
	         else if(str6[1].contains("E"))
	         {
	            return 4 % numReduceTasks;
	         }
	         else if(str6[1].contains("F"))
	         {
	            return 5 % numReduceTasks;
	         }
	         else if(str6[1].contains("G"))
	         {
	            return 6 % numReduceTasks;
	         }
	         else if(str6[1].contains("H"))
	         {
	            return 7 % numReduceTasks;
	         }
	         else if(str6[1].contains("I"))
	         {
	            return 8 % numReduceTasks;
	         }
	         else if(str6[1].contains("J"))
	         {
	            return 9 % numReduceTasks;
	         }
	         else 
	         {
	            return 10 % numReduceTasks;
	         }
	      }
	   }
		public static class SortReducer extends Reducer<DoubleWritable,Text,Text,DoubleWritable>
		{
			public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
			{
				for(Text val : value)
				{
					context.write(new Text(val), key);
				}
			}
		}
	  
	  
	  
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job1 = Job.getInstance(conf, "Volume Count");
		    job1.setJarByClass(retail_c12.class);
		    job1.setMapperClass(MapClass.class);
		    job1.setPartitionerClass(CaderPartitioner.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job1.setReducerClass(ReduceClass.class);
		    job1.setNumReduceTasks(11);
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(Text.class);
		    job1.setOutputKeyClass(LongWritable.class);
		    job1.setOutputValueClass(Text.class);
		    Path outputPath2 = new Path("FirstMapper");
		    FileInputFormat.addInputPath(job1, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job1, outputPath2);
		    FileSystem.get(conf).delete(outputPath2, true);
			job1.waitForCompletion(true);
			
			
			
			Job job3 = Job.getInstance(conf,"Per Occupation - totalTxn");
			job3.setJarByClass(retail_c12.class);
			job3.setMapperClass(SortMapper.class);
			job3.setPartitionerClass(CaderPartitioner1.class);
			job3.setReducerClass(SortReducer.class);
			job3.setNumReduceTasks(11);
			
			job3.setSortComparatorClass(DecreasingComparator.class);
			job3.setMapOutputKeyClass(DoubleWritable.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job3, outputPath2);
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			FileSystem.get(conf).delete(new Path(args[1]), true);
			System.exit(job3.waitForCompletion(true) ? 0 : 1);
		  }
}