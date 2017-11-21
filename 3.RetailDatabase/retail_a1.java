
import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class retail_a1 {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
		//private Text str1 = new Text();
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");
	            String str1 ="my_key";
	            //long vol = Long.parseLong(str[8]);
	            String str2 = str[0]+","+str[1]+","+str[8];
	            context.write(new Text(str1),new Text(str2));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
		    private Text result = new Text();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      long maxPercentValue = 0;
		      long temp_val = 0;
		      String st= "";
		         for ( Text val : values)
		        	
		         {   
		        	 String[] str3=val.toString().split(",");
		        	 LongWritable value = new LongWritable(Integer.parseInt(str3[2]));
		        	 
		        	 temp_val = value.get(); 
		 			if (temp_val > maxPercentValue) {
		 				maxPercentValue = temp_val;
		 				 st=str3[0]+","+str3[1]+","+maxPercentValue;
		 			}     
		         }
		      
		      
		        
		      result.set(st);
		      
		      context.write(key, result);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(StockVolume.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}