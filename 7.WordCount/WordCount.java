import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount extends Configured implements Tool{
	public int run(String[] args) throws Exception
	{
		
		JobConf conf = new JobConf(getConf(),WordCount.class);
		conf.setJobName("WordCount");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		return 0;
		
	}
	
	public static void main(String[] args)throws Exception
	{
		int res = ToolRunner.run(new Configuration(),new WordCount(),args);
		System.exit(res);
	}
}