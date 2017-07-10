package com.oarcle.mobile.phone.flow.format;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.oarcle.mobile.phone.flow.connect.JdbcManager;
import com.oarcle.mobile.phone.flow.mapper.dimention.FlowDimention;
import com.oarcle.mobile.phone.flow.mapper.dimention.UpDownFlowDimention;

public class MysqlFlowOutputFormat extends OutputFormat<FlowDimention, UpDownFlowDimention> {

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(arg0), arg0);
	}

	@Override
	public RecordWriter<FlowDimention, UpDownFlowDimention> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		Connection connection= JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf,connection);
		
	}
	
	public class MysqlRecordWriter extends RecordWriter<FlowDimention, UpDownFlowDimention>{
		private Configuration conf;
		private Connection connection;
		
		private HashMap<String, PreparedStatement> psMaps = new HashMap<String, PreparedStatement>();
		
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.conf = conf;
			this.connection = connection;
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			try {
				for(String psKey:psMaps.keySet()){
					PreparedStatement ps = psMaps.get(psKey);
					
						ps.executeBatch();
					} 
				connection.commit();
			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					for(String psKey:psMaps.keySet()){
						PreparedStatement ps = psMaps.get(psKey);
						
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
		
		private int count = 0;
		@Override
		public void write(FlowDimention key, UpDownFlowDimention value) throws IOException, InterruptedException {
			PreparedStatement ps = psMaps.get("phone_flow");
			if(ps == null){
				try {
					ps = connection.prepareStatement(conf.get("phone_flow"));
					psMaps.put("phone_flow", ps);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
			
			setPreparedStatement(ps, key, value);
			count++;
			if(count%10 == 0){
				try {
					ps.executeBatch();
					connection.commit();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				count = 0;
			}
		}
		
		public void setPreparedStatement(PreparedStatement ps,FlowDimention key,UpDownFlowDimention value){
			try {
				ps.setString(1, key.getPhoneDate());
				ps.setString(2, key.getPhoneNumber());
				ps.setString(3, String.valueOf(value.getUpFlow()));
				ps.setString(4, String.valueOf(value.getDownFlow()));
				ps.setString(5, String.valueOf(value.getTotalFlow()));
				ps.setString(6, String.valueOf(value.getUpFlow()));
				ps.setString(7, String.valueOf(value.getDownFlow()));
				ps.setString(8, String.valueOf(value.getTotalFlow()));
				
				ps.addBatch();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
	}

}
