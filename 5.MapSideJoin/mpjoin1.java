
import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class mpjoin1 {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
		private Map<String, String> abMap = new HashMap<String, String>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		    
		    FileSystem fs = FileSystem.get(context.getConfiguration());		    
		
			if (p.getName().equals("store_master")) {
//					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));

					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split(",");
						String str_id = tokens[0];
						String str_state = tokens[2];
						//String prd_qty = tokens[2];
						abMap.put(str_id, str_state);
						line = reader.readLine();
					}
					reader.close();
				}
			
		
			
			if (abMap.isEmpty()) {
				throw new IOException("MyError:Unable to load salary data.");
			}

		

		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	
        	String row = value.toString();//reading the data from Employees.txt
        	String[] tokens = row.split(",");
        	String str_id = tokens[0];
        	String prd_id = tokens[1];
        	String prd_qty = tokens[2];
        	String str_state1 = abMap.get(str_id);
       
        	
        	String sal_desig =prd_qty ; 
        	outputKey.set(tokens[1]);
        	outputValue.set(sal_desig);
      	  	context.write(outputKey,outputValue);
        }  
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,LongWritable>
	   {
		    private LongWritable result = new LongWritable();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      
		      long sum = 0;
		      
		         for ( Text val : values)
		        	
		         {   
		        	 String[] str3=val.toString().split(",");
		        	 LongWritable value = new LongWritable(Integer.parseInt(str3[0]));
		        	 sum +=value.get();     
		         }
		      
		      
		        
		      result.set(sum);
		      
		      context.write(key, result);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("mapreduce.output.textoutputformat.separator", ",");
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(mpjoin1.class);
		    job.setMapperClass(MapClass.class);
		    job.addCacheFile(new Path(args[1]).toUri());
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
