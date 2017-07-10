package com.oarcle.mobile.phone.flow.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oarcle.mobile.phone.flow.constants.DateType;
import com.oarcle.mobile.phone.flow.mapper.dimention.FlowDimention;
import com.oarcle.mobile.phone.flow.mapper.dimention.UpDownFlowDimention;
import com.oarcle.mobile.phone.flow.utils.DateUtils;

public class FlowMapper extends Mapper<LongWritable, Text, FlowDimention, UpDownFlowDimention> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, FlowDimention, UpDownFlowDimention>.Context context)
			throws IOException, InterruptedException {
		
		String line [] = value.toString().split("##");
		String phoneDate = DateUtils.toDate(line[0], DateType.DATE);
		String phoneNumber = line[1];
		String phoneUpFlow = line[8];
		String phoneDownFlow = line[9];
		
		FlowDimention flowDimention = new FlowDimention();
		flowDimention.setPhoneDate(phoneDate);
		flowDimention.setPhoneNumber(phoneNumber);
		
		UpDownFlowDimention upDownFlowDimention = new UpDownFlowDimention(
				Integer.parseInt(phoneUpFlow),Integer.parseInt(phoneDownFlow));
		
		context.write(flowDimention, upDownFlowDimention);
		
	}
}
