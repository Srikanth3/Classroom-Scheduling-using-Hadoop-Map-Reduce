
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class popular_department {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> 
	{
			private final static IntWritable one = new IntWritable(1);
			private Text word = new Text();
			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException 
			{
				String[] terms = value.toString().split(",");
				String hall_name  = terms[5];
				String dept = terms[4];
				try
				{
					int count_students = Integer.parseInt(terms[9]);
					if (hall_name.length() == 0) 
					{
						return;
					} 
					else 
					{
						hall_name = hall_name.trim();
						if (hall_name.equalsIgnoreCase("Unknown") || hall_name.equalsIgnoreCase("Arr")) 
						{
							return;
						}
					}
					word.set(dept);
					context.write(word, new IntWritable(count_students));
				}
				
				catch(NumberFormatException e)
				{
					return;
				}
				
			}
	}
	
	public static class intReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "popular course");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(intReducer.class);
		job.setReducerClass(intReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	

}

