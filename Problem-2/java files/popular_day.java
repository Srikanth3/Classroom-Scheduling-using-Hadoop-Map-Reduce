

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

public class popular_day {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> 
	{
			private final static IntWritable one = new IntWritable(1);
			private Text word = new Text();
			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException 
			{
				String[] terms = value.toString().split(",");
				String days  = terms[6];
				String hall = terms[5];
				//String course_name = terms[6];
				try
				{
					int count_students = Integer.parseInt(terms[9]);
					if (hall.length() == 0) 
					{
						return;
					} 
					else 
					{
						hall = hall.trim();
						days = days.trim();
						if (days.equalsIgnoreCase("UNKWN")  || hall.equalsIgnoreCase("UNKWN") || days.equalsIgnoreCase("ARR") || days.equalsIgnoreCase("Unknown")|| hall.equalsIgnoreCase("Unknown") || hall.equalsIgnoreCase("Arr")) 
						{
							return;
						}
					}
					//----------
					String[] indi_days = days.split("");
					for(String s : indi_days)
					{
						if(s.equals("M"))
						{
							word.set("M");
							context.write(word, new IntWritable(count_students));
						}
						if(s.equals("T"))
						{
							word.set("T");
							context.write(word, new IntWritable(count_students));
						}
						if(s.equals("W"))
						{
							word.set("W");
							context.write(word, new IntWritable(count_students));
						}
						if(s.equals("R"))
						{
							word.set("R");
							context.write(word, new IntWritable(count_students));
						}
						if(s.equals("F"))
						{
							word.set("F");
							context.write(word, new IntWritable(count_students));
						}
						if(s.equals("S"))
						{
							word.set("S");
							context.write(word, new IntWritable(count_students));
						}
					}
					
					
					//--------
					//word.set(days);
					//context.write(word, new IntWritable(count_students));
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

