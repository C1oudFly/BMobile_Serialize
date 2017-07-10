package com.oarcle.mobile.phone.flow.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.oarcle.mobile.phone.flow.mapper.dimention.SiteVisitCountDimention;

public class SiteVisitReducer extends Reducer<SiteVisitCountDimention, IntWritable, SiteVisitCountDimention, IntWritable> {
	
	@Override
	protected void reduce(SiteVisitCountDimention keys, Iterable<IntWritable> values,
			Reducer<SiteVisitCountDimention, IntWritable, SiteVisitCountDimention, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		
		for(IntWritable value:values){
			count += value.get();
		}
		context.write(keys, new IntWritable(count));
	}
}
