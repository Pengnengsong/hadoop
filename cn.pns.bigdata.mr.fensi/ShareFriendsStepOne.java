
package cn.pns.bigdata.mr.fensi;

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



public class ShareFriendsStepOne {

	
	static class ShareFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] person_friends = line.split(":");
			String person = person_friends[0];
			String friends = person_friends[1];
			
			for(String friend:friends.split(",")){
				context.write(new Text(friend), new Text(person));
			}
		}
	}
	
	static class ShareFriendsStepOneReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {

			StringBuffer sb = new StringBuffer();
			
			for(Text person:persons){
				sb.append(person).append(",");
			}
			context.write(friend, new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(ShareFriendsStepOne.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(ShareFriendsStepOneMapper.class);
		job.setReducerClass(ShareFriendsStepOneReducer.class);
		
		FileInputFormat.setInputPaths(job, new Path("G:/wordcount/friend"));
		FileOutputFormat.setOutputPath(job, new Path("G:/wordcount/friend/out1"));
		
		job.waitForCompletion(true);
		
	}
	
}

