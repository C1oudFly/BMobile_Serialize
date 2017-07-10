package com.oarcle.mobile.phone.flow.mapper.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SiteVisitCountDimention implements WritableComparable<SiteVisitCountDimention> {
	
	private String phoneDate;
	private String phoneNumber;
	private String phoneSite;
	
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

	public String getPhoneSite() {
		return phoneSite;
	}

	public void setPhoneSite(String phoneSite) {
		this.phoneSite = phoneSite;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.phoneDate);
		out.writeUTF(this.phoneNumber);
		out.writeUTF(this.phoneSite);
	}

	public void readFields(DataInput in) throws IOException {
		this.phoneDate = in.readUTF();
		this.phoneNumber = in.readUTF();
		this.phoneSite = in.readUTF();
	}

	public int compareTo(SiteVisitCountDimention o) {
		if(this == o){
			return 0;
		}
		
		int temp = this.phoneDate.compareTo(o.phoneDate);
		if(temp != 0){
			return temp;
		}
		
		temp = this.phoneNumber.compareTo(o.phoneDate);
		if(temp != 0){
			return temp;
		}
		
		temp = this.phoneSite.compareTo(o.phoneSite);
		if(temp != 0){
			return temp;
		}
		
		return 0;
	}
	
	@Override
	public int hashCode() {
		int result = 1;
		int prime = 100;
		
		result = (result*prime) + phoneDate == null ? 0 : phoneDate.hashCode();
		result = (result*prime) + phoneNumber ==  null ? 0 : phoneNumber.hashCode();
		result = (result*prime) + phoneSite == null ? 0 : phoneSite.hashCode();
		
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this.equals(obj)){
			return true;
		}
		
		if(this.getClass() != obj){
			return false;
		}
		
		SiteVisitCountDimention sCountDimention = (SiteVisitCountDimention) obj;
		
		if(this.phoneDate == null){
			if(sCountDimention.phoneDate != null){
				return false;
			}
		}else if(sCountDimention.phoneDate == null){
			if(this.phoneDate != null){
				return false;
			}
		}else if (!this.phoneDate.equals(sCountDimention.phoneDate)) {
			return false;
		}
		
		if(this.phoneNumber == null){
			if(sCountDimention.phoneNumber != null){
				return false;
			}
		}else if(sCountDimention.phoneNumber == null){
			if(this.phoneNumber != null){
				return false;
			}
		}else if (!this.phoneNumber.equals(sCountDimention.phoneNumber)) {
			return false;
		}
		
		if(this.phoneSite == null){
			if(sCountDimention.phoneSite != null){
				return false;
			}
		}else if(sCountDimention.phoneSite == null){
			if(this.phoneSite != null){
				return false;
			}
		}else if (!this.phoneSite.equals(sCountDimention.phoneSite)) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.phoneDate + "\t" + this.phoneNumber + "\t" + this.phoneSite;
	}

}
