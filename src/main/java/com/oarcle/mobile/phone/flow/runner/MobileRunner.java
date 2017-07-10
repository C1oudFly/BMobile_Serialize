package com.oarcle.mobile.phone.flow.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oarcle.mobile.date.mobile.parttitioner.MobilePartitioner;
import com.oarcle.mobile.phone.flow.mapper.MobileMapper;
import com.oarcle.mobile.phone.flow.mapper.dimention.FlowNetCountValue;
import com.oarcle.mobile.phone.flow.mapper.dimention.MobileDimention;
import com.oarcle.mobile.phone.flow.reducer.MobileReducer;

public class MobileRunner implements Tool {
	
	private Configuration conf;
	
	public void setConf(Configuration conf) {
		this.conf= conf;
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(MobileRunner.class);
		
		job.setPartitionerClass(MobilePartitioner.class);
		job.setNumReduceTasks(3);
		
		job.setMapperClass(MobileMapper.class);
		job.setMapOutputKeyClass(MobileDimention.class);
		job.setMapOutputValueClass(FlowNetCountValue.class);
		
		job.setReducerClass(MobileReducer.class);
		job.setOutputKeyClass(MobileDimention.class);
		job.setOutputValueClass(FlowNetCountValue.class);
		
		//job.setOutputFormatClass(MysqlFlowOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/mobile.dat"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://yunfei1:9000/flow1"));
		
		if(job.waitForCompletion(true)){
			return 1;
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new MobileRunner(), args);
		System.out.println(result);
	}
}
