package com.oarcle.mobile.phone.flow.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oarcle.mobile.phone.flow.format.MysqlFlowOutputFormat;
import com.oarcle.mobile.phone.flow.mapper.FlowMapper;
import com.oarcle.mobile.phone.flow.mapper.dimention.FlowDimention;
import com.oarcle.mobile.phone.flow.mapper.dimention.UpDownFlowDimention;
import com.oarcle.mobile.phone.flow.reducer.FlowReducer;

public class PhoneFlowRunner implements Tool {

	private Configuration conf;

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		conf.addResource("jdbc.xml");
		conf.addResource("sql_mapper.xml");
		this.conf = conf;
	}

	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(PhoneFlowRunner.class);
		
		job.setMapperClass(FlowMapper.class);
		job.setMapOutputKeyClass(FlowDimention.class);
		job.setMapOutputValueClass(UpDownFlowDimention.class);
		
		job.setReducerClass(FlowReducer.class);
		job.setOutputKeyClass(FlowDimention.class);
		job.setOutputValueClass(UpDownFlowDimention.class);
		
		job.setOutputFormatClass(MysqlFlowOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/mobile.dat"));
		//FileOutputFormat.setOutputPath(job, new Path("hdfs://yunfei1:9000/flow"));
		
		if(job.waitForCompletion(true)){
			return 1;
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new PhoneFlowRunner(), args);
		System.out.println(result);
	}

}
