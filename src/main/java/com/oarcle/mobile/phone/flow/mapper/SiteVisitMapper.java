package com.oarcle.mobile.phone.flow.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oarcle.mobile.phone.flow.constants.DateType;
import com.oarcle.mobile.phone.flow.mapper.dimention.SiteVisitCountDimention;
import com.oarcle.mobile.phone.flow.utils.DateUtils;

public class SiteVisitMapper extends Mapper<LongWritable, Text, SiteVisitCountDimention, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, SiteVisitCountDimention, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String lines [] = value.toString().split("##");
		
		String phoneDate = DateUtils.toDate(lines[0], DateType.DATE);
		String phoneNumber = lines[1];
		String phoneSite = lines[4];
		
		if(phoneSite.equals("")){
			;
		}else{
			SiteVisitCountDimention sCountDimention = new SiteVisitCountDimention();
			sCountDimention.setPhoneDate(phoneDate);
			sCountDimention.setPhoneNumber(phoneNumber);
			sCountDimention.setPhoneSite(phoneSite);
			System.out.println(sCountDimention.toString());
			context.write(sCountDimention, new IntWritable(1));
		}
		
	}

}
