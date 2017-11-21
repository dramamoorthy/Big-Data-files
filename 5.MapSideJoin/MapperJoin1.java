import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;





public class MapperJoin1 {
   
   
    public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
       
       
        private Map<String, String> abMap = new HashMap<String, String>();
       
        private Text outputKey = new Text();
        private Text outputValue = new Text();
       
        protected void setup(Context context) throws java.io.IOException, InterruptedException{
           
            super.setup(context);

            URI[] files = context.getCacheFiles(); // getCacheFiles returns null

            Path p = new Path(files[0]);
       
           // Path p1 = new Path(files[1]);
           
            FileSystem fs = FileSystem.get(context.getConfiguration());           
       
            if (p.getName().equals("store_master")) {
//                    BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));

                    String line = reader.readLine();
                    while(line != null) {
                        String[] tokens = line.split(",");
                        String str_id = tokens[0];
                        String city = tokens[2];
                        abMap.put(str_id, city);
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
            String store_id = tokens[0];
            String prod_id = tokens[1];
            String prod_qty = tokens[2];
            String sto_id = abMap.get(store_id);
            //String desig = abMap1.get(emp_id);
            String store = prod_qty;//+","+sto_id;
            outputKey.set(tokens[1]);
            outputValue.set(store);
                context.write(outputKey,outputValue);
        } 
       
}
    public static class ReduceClass extends Reducer<Text,Text,Text,LongWritable>
       {
            private LongWritable result = new LongWritable();
            long sum = 0;
            public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
             

               
               
                 for (Text val : values)
                 {          
                     String row1 = val.toString();
                        String[] tokens = row1.split(",");
                        LongWritable value = new LongWritable(Integer.parseInt(tokens[0]));
                    sum += value.get();     
                 }
                
              result.set(sum);             
              context.write(key, result);
              //context.write(key, new LongWritable(sum));
             
            }
       }
   
  public static void main(String[] args)
                  throws IOException, ClassNotFoundException, InterruptedException {
   
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    Job job = Job.getInstance(conf);
    job.setJarByClass(MapperJoin1.class);
    job.setJobName("Map Side Join");
  
   
    job.setMapperClass(MyMapper.class);
    job.addCacheFile(new Path(args[1]).toUri());
    job.setCombinerClass(ReduceClass.class);
   // job.addCacheFile(new Path(args[2]).toUri());
    //job.setPartitionerClass(CaderPartitioner.class);
    job.setReducerClass(ReduceClass.class);
    //job.setNumReduceTasks(0);
   
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
   
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
   
    job.waitForCompletion(true);
   
   
  }
}