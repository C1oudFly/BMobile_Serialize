package com.oarcle.mobile.phone.flow.mapper.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 实现WritableComparable
 * @author Administrator
 * 序列化概念：使用了自己序列化机制
 */

public class FlowDimention implements WritableComparable<FlowDimention> {
	
	private String phoneDate;
	private String phoneNumber;
	
	public String getPhoneDate() {
		return phoneDate;
	}

	public void setPhoneDate(String phoneDate) {
		this.phoneDate = phoneDate;
	}

	public String getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	
	public void readFields(DataInput in) throws IOException {
		
		this.phoneDate = in.readUTF();
		this.phoneNumber = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.phoneDate);
		out.writeUTF(this.phoneNumber);
		
	}

	int count = 0;
	public int compareTo(FlowDimention o) {
		if(this == o){
			return 0;
		}
		
		int tmp = this.phoneDate.compareTo(o.phoneDate);
		if(tmp != 0){
			return tmp;
		}
		tmp = this.phoneNumber.compareTo(o.phoneNumber);
		
		if(tmp != 0){
			return tmp;
		}
		
		return 0;
	}
	
	@Override
	public int hashCode() {
		int  result = 1;
		int prime = 100;
		
		result = (result*prime) + phoneDate == null ? 0 : phoneDate.hashCode();
		result = (result*prime) + phoneNumber == null ? 0 : phoneNumber.hashCode();
		
		return result;
		
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this == obj){
			return true;
		}
		
		if(getClass() != obj.getClass()){
			return false;
		}
		
		FlowDimention flowDimention = (FlowDimention) obj;
		
		if(this.phoneDate == null){
			if(flowDimention.phoneDate != null){
				return false;
			}
		}else if (this.phoneDate != null) {
			if(flowDimention.phoneDate == null){
				return false;
			}
		}else if (!this.phoneDate.equals(flowDimention.phoneDate)) {
			return false;
		}
		
		if(this.phoneNumber == null){
			if(flowDimention.phoneNumber != null){
				return false;
			}
		}else if (this.phoneNumber != null) {
			if(flowDimention.phoneNumber == null){
				return false;
			}
		}else if (!this.phoneNumber.equals(flowDimention.phoneNumber)) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.phoneDate + "\t" + this.phoneNumber;
	}

}
