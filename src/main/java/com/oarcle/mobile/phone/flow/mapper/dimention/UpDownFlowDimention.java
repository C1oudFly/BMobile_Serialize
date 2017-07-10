package com.oarcle.mobile.phone.flow.mapper.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UpDownFlowDimention implements Writable {
	
	private int upFlow;
	private int downFlow;
	private int totalFlow;
	
	public int getTotalFlow() {
		return totalFlow;
	}

	public void setTotalFlow(int totalFlow) {
		this.totalFlow = totalFlow;
	}

	public int getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(int upFlow) {
		this.upFlow = upFlow;
	}

	public int getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(int downFlow) {
		this.downFlow = downFlow;
	}

	public UpDownFlowDimention() {
		// TODO Auto-generated constructor stub
	}
	
	public UpDownFlowDimention(int upFlow, int downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.totalFlow = upFlow + downFlow;
	}

	public void readFields(DataInput in) throws IOException {
		
		this.upFlow = in.readInt();
		this.downFlow = in.readInt();
		this.totalFlow = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		
		out.writeInt(upFlow);
		out.writeInt(downFlow);
		out.writeInt(totalFlow);

	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.upFlow + "\t" + this.downFlow + "\t" + this.totalFlow;
	}

}
