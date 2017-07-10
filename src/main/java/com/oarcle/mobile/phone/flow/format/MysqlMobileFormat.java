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

import com.oarcle.mobile.date.mobile.collector.BaseCollector;
import com.oarcle.mobile.phone.flow.connect.JdbcManager;
import com.oarcle.mobile.phone.flow.mapper.dimention.FlowNetCountValue;
import com.oarcle.mobile.phone.flow.mapper.dimention.MobileDimention;

public class MysqlMobileFormat extends OutputFormat<MobileDimention, FlowNetCountValue> {

	@Override
	public RecordWriter<MobileDimention, FlowNetCountValue> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(conf);
		System.out.println("我是format==================================================================");
		return new MysqlRecordWriter(conf,connection);
	}
	
	class MysqlRecordWriter extends RecordWriter<MobileDimention, FlowNetCountValue>{
		HashMap<String, PreparedStatement> psMaps = new HashMap<String, PreparedStatement>();
		Configuration conf;
		Connection connection;
		
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.conf = conf;
			this.connection = connection;
		}

		int count = 0;
		@Override
		public void write(MobileDimention key, FlowNetCountValue value) throws IOException, InterruptedException {
			//=============================================创建PrepareStatement======================================
			PreparedStatement ps = psMaps.get("mobile" + key.getType());
			if(ps == null){
				try {
					ps = connection.prepareStatement(conf.get("mobile_" + key.getType()));
					psMaps.put("mobile" + key.getType(), ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			count++;
			String classPath = conf.get("collector_" + key.getType());
			try {
				BaseCollector baseCollector = (BaseCollector)Class.forName(classPath).newInstance();
				baseCollector.setPreparedStatement(ps, key, value);
				
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if(count % 10 == 0){
				
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

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			try {
				for(String key:psMaps.keySet()){
					PreparedStatement ps = psMaps.get(key);
					ps.executeBatch(); 
				}
				connection.commit();
			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					for(String key:psMaps.keySet()){
						PreparedStatement ps = psMaps.get(key);
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
