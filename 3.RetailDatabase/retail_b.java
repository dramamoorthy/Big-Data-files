
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
import java.util.TreeMap;


public class retail_b {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
		private TreeMap<Text,Long>productData=new TreeMap<Text,Long>();
		//private Text str1 = new Text();
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");
	            long sales = Long.parseLong(str[8]);
	            productData.put(new Text(str[5]), sales);
	            if (productData.size()>10)
	            {
	            	productData.remove(productData.firstKey());
	            }
	            //context.write(new Text(str[5]),new LongWritable(sales));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	         
	      }
	      protected void cleanup(Context context)throws IOException
	      {
	    	  for(Map.Entry<Text,Long>entry:productData.entrySet())
	    	  {
	    		  context.write(entry.getKey(),entry.getValue());
	    	  }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
	   {
		    private LongWritable result = new LongWritable();
		    
		    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		      
		      long sum = 0;
		      
		         for ( LongWritable val : values)
		        	
		         {   
		        	 sum +=val.get();     
		         }
		      
		      
		        
		      result.set(sum);
		      
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
		    job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}

