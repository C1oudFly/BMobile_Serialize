package com.oarcle.mobile.phone.flow.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oarcle.mobile.date.mobile.constants.MobileConstants;
import com.oarcle.mobile.phone.flow.constants.DateType;
import com.oarcle.mobile.phone.flow.mapper.dimention.DatePhoneDimention;
import com.oarcle.mobile.phone.flow.mapper.dimention.FlowNetCountValue;
import com.oarcle.mobile.phone.flow.mapper.dimention.MobileDimention;
import com.oarcle.mobile.phone.flow.utils.DateUtils;

public class MobileMapper extends Mapper<LongWritable, Text, MobileDimention, FlowNetCountValue> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, MobileDimention, FlowNetCountValue>.Context context)
			throws IOException, InterruptedException {
		
		String lines [] = value.toString().split("##");
		
		String phoneDate = lines[0];
		String phoneNumber = lines[1];
		String netAddress = lines[4];
		String upFlow = lines[8];
		String downFlow = lines[9];
		
		//========================做流量计算输出===================================================
		MobileDimention mobileDimention = new MobileDimention();
		DatePhoneDimention datePhoneDimention = new DatePhoneDimention();
		
		datePhoneDimention.setPhoneDate(DateUtils.toDate(phoneDate, DateType.DATE));
		datePhoneDimention.setPhoneNumber(phoneNumber);
		
		mobileDimention.setDatePhoneDimention(datePhoneDimention);
		mobileDimention.setNetAddress("");
		mobileDimention.setType(MobileConstants.FLOWTYPE);
		
		FlowNetCountValue flowNetCountValue = new FlowNetCountValue(Integer.parseInt(upFlow), 
				Integer.parseInt(upFlow), 0);
		context.write(mobileDimention, flowNetCountValue);
		//========================做流量计算输出===================================================
		
		//========================做网址计算输出===================================================
		if(netAddress != null && (!netAddress.equals(""))){
			mobileDimention.setNetAddress(netAddress);
			mobileDimention.setType(MobileConstants.NETTYPE);
			flowNetCountValue.setCount(1);
			context.write(mobileDimention, flowNetCountValue);
		}
		//========================做网址计算输出===================================================
	
	}
}
