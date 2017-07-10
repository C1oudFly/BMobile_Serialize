package com.oarcle.mobile.phone.flow.reducer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import com.oarcle.mobile.phone.flow.mapper.dimention.FlowDimention;
import com.oarcle.mobile.phone.flow.mapper.dimention.UpDownFlowDimention;

public class FlowReducer extends Reducer<FlowDimention, UpDownFlowDimention, FlowDimention, UpDownFlowDimention> {
	@Override
	protected void reduce(FlowDimention keys, Iterable<UpDownFlowDimention> values,
			Reducer<FlowDimention, UpDownFlowDimention, FlowDimention, UpDownFlowDimention>.Context context)
			throws IOException, InterruptedException {
		
		int upFlow = 0;
		int downFlow = 0;
		
		for(UpDownFlowDimention upDown : values){
			upFlow += upDown.getUpFlow();
			downFlow += upDown.getDownFlow();
		}
		
		UpDownFlowDimention upDownFlowDimention = new UpDownFlowDimention(upFlow,downFlow);
		context.write(keys,upDownFlowDimention);
	}
}
