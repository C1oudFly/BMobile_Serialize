package com.oarcle.mobile.phone.flow.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oarcle.mobile.phone.flow.format.MysqlSiteVisitOutputFormat;
import com.oarcle.mobile.phone.flow.mapper.SiteVisitMapper;
import com.oarcle.mobile.phone.flow.mapper.dimention.SiteVisitCountDimention;
import com.oarcle.mobile.phone.flow.reducer.SiteVisitReducer;

public class SiteVistRunner implements Tool {
	private Configuration conf;
	
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
		job.setJarByClass(SiteVistRunner.class);
		
		job.setMapperClass(SiteVisitMapper.class);
		job.setMapOutputKeyClass(SiteVisitCountDimention.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(SiteVisitReducer.class);
		job.setOutputKeyClass(SiteVisitCountDimention.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/mobile.dat"));
		job.setOutputFormatClass(MysqlSiteVisitOutputFormat.class);
		if(job.waitForCompletion(true)){
			return 1;
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new SiteVistRunner(), args);
		System.out.println(result);

	}

}
