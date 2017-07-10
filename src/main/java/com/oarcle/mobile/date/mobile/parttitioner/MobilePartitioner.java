package com.oarcle.mobile.date.mobile.parttitioner;

import org.apache.hadoop.mapreduce.Partitioner;

import com.oarcle.mobile.phone.flow.mapper.dimention.FlowNetCountValue;
import com.oarcle.mobile.phone.flow.mapper.dimention.MobileDimention;

public class MobilePartitioner extends Partitioner<MobileDimention, FlowNetCountValue> {

	@Override
	public int getPartition(MobileDimention key, FlowNetCountValue value, int numPartitions) {
		
		
		return key.getType() == 1 ? 0 : 1;
	}

}
