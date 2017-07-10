package com.oarcle.mobile.phone.flow.format;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.oarcle.mobile.phone.flow.connect.JdbcManager;
import com.oarcle.mobile.phone.flow.mapper.dimention.PvDimention;

public class MysqlPvOutputformat extends OutputFormat<PvDimention, IntWritable> {

	@Override
	public RecordWriter<PvDimention, IntWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf,connection);
	}
	
	class MysqlRecordWriter extends RecordWriter<PvDimention, IntWritable>{
		
		private Configuration conf;
		private Connection connection;
		private HashMap<String, PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
		
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.conf = conf;
			this.connection = connection;
		}

		private int count = 0;
		@Override
		public void write(PvDimention key, IntWritable value) throws IOException, InterruptedException {
			PreparedStatement ps = psMap.get("pv_count");
			
			if(ps == null){
				try {
					ps = connection.prepareStatement(conf.get("pv_count"));
					psMap.put("pv_count", ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			setPreparedStatement(ps,key,value);
			count++;
			if(count % 10 == 0){
				try {
					ps.executeBatch();
					connection.commit();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			count = 0;
		}

		private void setPreparedStatement(PreparedStatement ps, PvDimention key, IntWritable value) {
			try {
				ps.setString(1, key.getPhoneDate());
				ps.setString(2, key.getPhoneNumber());
				ps.setString(3, Integer.toString(value.get()));
				ps.setString(4, Integer.toString(value.get()));
				
				ps.addBatch();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			try {
				
				for(String psKey:psMap.keySet()){
					PreparedStatement ps = psMap.get(psKey);
					ps.executeBatch();
				} 
				connection.commit();
			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					
					for(String psKey:psMap.keySet()){
						PreparedStatement ps = psMap.get(psKey);
						ps.close();
					} 
				}catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}finally {
					try {
						connection.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}

}
