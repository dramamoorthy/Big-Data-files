

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




  public class retail_c2atree {
		public static class Top5Mapper extends
		Mapper<LongWritable, Text, Text, Text> {
			public void map(LongWritable key, Text value, Context context
	        ) throws IOException, InterruptedException {
				try {
				String[] str = value.toString().split(";");
				String custid= str[4];
				long cost = Long.parseLong(str[7]);
	            
				long sale= Long.parseLong(str[8]);
				long viable = cost-sale;
				String str2 = str[2]+","+viable;
				context.write(new Text(custid),new Text(str2));
		           
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
	

		public static class Top5Reducer extends
		Reducer<Text, Text, NullWritable, Text> {
			private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

			public void reduce(Text key, Iterable<Text> values,
					Context context) throws IOException, InterruptedException {
				long sum=0;
				String myvalue= "";
				String mysum= "";
					for (Text val : values) {
						//sum+= val.get();
						
						 String[] str3=val.toString().split(",");
			        	 LongWritable value = new LongWritable(Integer.parseInt(str3[1]));
			        	 
			        	 sum += value.get();
					}
					myvalue= key.toString();
					mysum= String.format("%d", sum);
					myvalue= myvalue + ',' + mysum;
					repToRecordMap.put(new Long(sum), new Text(myvalue));
					if (repToRecordMap.size() > 5) {
					repToRecordMap.remove(repToRecordMap.firstKey());
							}
			}
			
					protected void cleanup(Context context) throws IOException,
					InterruptedException {
					for (Text t : repToRecordMap.values()) {
						// Output our five records to the file system with a null key
						context.write(NullWritable.get(), t);
						}
					}
			
		}
			
		public static void main(String[] args) throws Exception {
			
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Top record for largest amount spent");
		    job.setJarByClass(retail_c2atree.class);
		    job.setMapperClass(Top5Mapper.class);
		    job.setPartitionerClass(CaderPartitioner.class);
		    //job.setCombinerClass(ReduceClass.class);
		    //job1.setReducerClass(ReduceClass.class);
		    //job1.setNumReduceTasks(11);
		    job.setReducerClass(Top5Reducer.class);
		    //job.setSortComparatorClass(DecreasingComparator.class);
		    job.setNumReduceTasks(11);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	}