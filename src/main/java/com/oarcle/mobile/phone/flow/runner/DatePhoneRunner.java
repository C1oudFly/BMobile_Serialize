package com.oarcle.mobile.phone.flow.runner;

import java.sql.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oarcle.mobile.phone.flow.format.MysqlMobileFormat;
import com.oarcle.mobile.phone.flow.mapper.MobileMapper;
import com.oarcle.mobile.phone.flow.mapper.dimention.FlowNetCountValue;
import com.oarcle.mobile.phone.flow.mapper.dimention.MobileDimention;
import com.oarcle.mobile.phone.flow.reducer.MobileReducer;

public class DatePhoneRunner implements Tool {
	private Configuration conf;
	private Connection connection;
	public void setConf(Configuration conf) {
		this.conf = conf;
		conf.addResource("jdbc.xml");
		conf.addResource("sql_mapper.xml");
		conf.addResource("sql_collector.xml");
	}

	public Configuration getConf() {
		return conf;
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(DatePhoneRunner.class);
		
		job.setMapperClass(MobileMapper.class);
		job.setMapOutputKeyClass(MobileDimention.class);
		job.setMapOutputValueClass(FlowNetCountValue.class);
		
		job.setReducerClass(MobileReducer.class);
		job.setOutputKeyClass(MobileDimention.class);
		job.setOutputValueClass(FlowNetCountValue.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/mobile.dat"));
		job.setOutputFormatClass(MysqlMobileFormat.class);
		if(job.waitForCompletion(true)){
			return 1;
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new DatePhoneRunner(), args);
		System.out.println(result);
	}
}
