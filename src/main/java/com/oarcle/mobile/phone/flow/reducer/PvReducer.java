package com.oarcle.mobile.phone.flow.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.oarcle.mobile.phone.flow.mapper.dimention.PvDimention;

public class PvReducer extends Reducer<PvDimention, IntWritable, PvDimention, IntWritable> {
	@Override
	protected void reduce(PvDimention keys, Iterable<IntWritable> values,
			Reducer<PvDimention, IntWritable, PvDimention, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		for(IntWritable value:values){
			count += value.get();
		}
		System.out.println(keys.toString() + count);
		context.write(keys, new IntWritable(count));
	}
}
