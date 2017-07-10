package com.oarcle.mobile.phone.flow.runner;

import java.sql.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oarcle.mobile.phone.flow.format.MysqlPvOutputformat;
import com.oarcle.mobile.phone.flow.mapper.PvMapper;
import com.oarcle.mobile.phone.flow.mapper.dimention.PvDimention;
import com.oarcle.mobile.phone.flow.reducer.PvReducer;

public class PvRunner implements Tool {
	private Configuration conf;
	private Connection connection;
	public void setConf(Configuration conf) {
		this.conf = conf;
		conf.addResource("jdbc.xml");
		conf.addResource("sql_mapper.xml");
	}

	public Configuration getConf() {
		return conf;
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(PvRunner.class);
		
		job.setMapperClass(PvMapper.class);
		job.setMapOutputKeyClass(PvDimention.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(PvReducer.class);
		job.setOutputKeyClass(PvDimention.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/mobile.dat"));
		job.setOutputFormatClass(MysqlPvOutputformat.class);
		if(job.waitForCompletion(true)){
			return 1;
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new PvRunner(), args);
		System.out.println(result);
	}

}
