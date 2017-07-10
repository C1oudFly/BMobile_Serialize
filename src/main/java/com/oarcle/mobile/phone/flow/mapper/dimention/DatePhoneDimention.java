package com.oarcle.mobile.phone.flow.mapper.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DatePhoneDimention implements WritableComparable<DatePhoneDimention> {
	
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

	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phoneDate);
		out.writeUTF(phoneNumber);
	}

	public void readFields(DataInput in) throws IOException {
		this.phoneDate = in.readUTF();
		this.phoneNumber = in.readUTF();
	}

	public int compareTo(DatePhoneDimention o) {
		if(this == o){
			return 0;
		}
		
		int temp = this.phoneDate.compareTo(o.phoneDate);
		if(temp != 0){
			return temp;
		}
		
		temp = this.phoneNumber.compareTo(o.phoneNumber);
		if(temp != 0){
			return temp;
		}
		return 0;
	}
	
	@Override
	public int hashCode() {
		int result = 1;
		int prime = 100;
		result = (result*prime) + this.phoneDate == null ? 0 : this.phoneDate.hashCode();
		result = (result*prime) + this.phoneNumber == null ? 0 : this.phoneNumber.hashCode();
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this == obj){
			return true;
		}
		
		if(this.getClass() != obj.getClass()){
			return false;
		}
		
		DatePhoneDimention dimention = (DatePhoneDimention) obj;
		
		if(this.phoneDate == null){
			if(dimention.phoneDate != null){
				return false;
			}
		}else if(dimention.phoneDate == null) {
			if(this.phoneDate != null){
				return false;
			}
		}else if (!this.phoneDate.equals(dimention.phoneDate)) {
			return false;
		}
		
		if(this.phoneNumber == null){
			if(dimention.phoneNumber != null){
				return false;
			}
		}else if(dimention.phoneNumber == null) {
			if(this.phoneNumber != null){
				return false;
			}
		}else if (!this.phoneNumber.equals(dimention.phoneNumber)) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		return this.phoneDate + "\t" + this.phoneNumber;
	}

}
