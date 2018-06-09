
package cn.pns.bigdata.mr.inverindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InverIndexStepTwo {
	
	static class InverIndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			
			String[] word_file = line.split("--");
			context.write(new Text(word_file[0]), new Text(word_file[1]));
		}
	}
	
	
	static class InverIndexStepTwoReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text value:values){
				sb.append(value.toString()+",");
			}
			
			context.write(key, new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(InverIndexStepTwo.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path("G:/wordcount/inverindexoutput1"));
		FileOutputFormat.setOutputPath(job, new Path("G:/wordcount/inverindexoutput2"));
		// FileInputFormat.setInputPaths(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(InverIndexStepTwoMapper.class);
		job.setReducerClass(InverIndexStepTwoReducer.class);

		job.waitForCompletion(true);

	}
	
	

}

