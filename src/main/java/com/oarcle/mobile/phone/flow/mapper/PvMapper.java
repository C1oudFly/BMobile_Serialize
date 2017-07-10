package com.oarcle.mobile.phone.flow.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oarcle.mobile.phone.flow.constants.DateType;
import com.oarcle.mobile.phone.flow.mapper.dimention.PvDimention;
import com.oarcle.mobile.phone.flow.utils.DateUtils;

public class PvMapper extends Mapper<LongWritable, Text, PvDimention, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, PvDimention, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String lines [] = value.toString().split("##");
		String phoneDate = DateUtils.toDate(lines[0], DateType.DATE);
		String phoneNumber = lines[1];
		String phoneSite = lines[4];
		
		if(phoneSite.equals("")){
			;
		}else {
			PvDimention pvDimention = new PvDimention();
			pvDimention.setPhoneDate(phoneDate);
			pvDimention.setPhoneNumber(phoneNumber);
			System.out.println(pvDimention.getPhoneDate() +" \t" + pvDimention.getPhoneNumber());
			context.write(pvDimention, new IntWritable(1));
		}
	}
}
