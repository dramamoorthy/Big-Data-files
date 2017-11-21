
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


public class retail_a3a {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
		//private Text str1 = new Text();
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");
	            //long sales = Long.parseLong(str[8]);
	            //long cost = Long.parseLong(str[7]);
	            //long profit = sales-cost;
	            //long profit_percentage=(profit*100);
	            //long profit_percentage_final=(profit_percentage/cost);
	            String str2 = str[7]+","+str[8];
	            context.write(new Text(str[5]),new Text(str2));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
		    //private LongWritable result = new LongWritable();
		  private Text result = new Text();
		    public long profit=0;
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      
		      long sumcost = 0;
		      long sumsale = 0;
		      long cost1=0;
		      long sale1=0;
		      long profitpercent1 = 0;
		      double profitpercentfinal=0;
		      String stfinal="";
		      //String stfinal1="";
		      //long profit=0;
		      
		      for ( Text val : values)
		        	
		         {   
		        	 //String[] str3=val.toString().split(",");
		        	 //LongWritable cost1 = new LongWritable(Integer.parseInt(str3[0]));
		        	 //LongWritable sale1 = new LongWritable(Integer.parseInt(str3[1]));
		        	 //profit=sale1-cost1;
		        	 
		        	 String[] str4 = val.toString().split(",");	 
			             cost1 = Long.parseLong(str4[0]);
			             sale1 = Long.parseLong(str4[1]);
			             sumcost += cost1;
			             sumsale += sale1;
			             profit = sumsale-sumcost;
			             profitpercent1= profit*100;
			             profitpercentfinal = profitpercent1/sumcost;
		        	 
		 				 //st=str3[0]+","+str3[1]+","+maxPercentValue;
		 			}     
		         
		      //profit = sale1-cost1;
		      //stfinal=String.format("%d",profitpercentfinal);
		      //stfinal1 = stfinal+"%";
		      stfinal=profitpercentfinal+"%";
		      result.set(stfinal);  
		     
		      
		      context.write(key,result);
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

