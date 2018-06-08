
package cn.pns.bigdata.mr.flowssum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowCountSort {

	static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
		
		FlowBean bean = new FlowBean();
		Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 拿到上一个统计程序的输出结果
			String line = value.toString();
			
			String[] fields = line.split("\t");
			
			String phoneNB = fields[0];
			
			long upFlow = Long.parseLong(fields[1]);
			
			long dFlow = Long.parseLong(fields[2]);
			
			bean.setFlowBean(upFlow, dFlow);
			v.set(phoneNB);
			
			context.write(bean, v);
		}
	}
	
	static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
		
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			context.write(values.iterator().next(), bean);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		/*job.setJar("/home/hadoop/wc.jar");*/
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(FlowCountSort.class);
		
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);
		
		/*//指定自定义的数据分区器
		job.setPartitionerClass(ProvincePartition.class);
		//同时指定相应分区数量的reducetask
		job.setNumReduceTasks(5);*/
		
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//指定job的输入/输出目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		Path outPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
	
		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/*job.submit();*/
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
	
	
}

