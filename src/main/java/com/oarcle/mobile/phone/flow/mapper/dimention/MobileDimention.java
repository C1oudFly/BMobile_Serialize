package com.oarcle.mobile.phone.flow.mapper.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * 创建维度
 * @author Administrator
 *
 */
public class MobileDimention implements WritableComparable<MobileDimention> {
	
	private DatePhoneDimention datePhoneDimention = new DatePhoneDimention();
	private String netAddress;
	private Integer type;
	
	public DatePhoneDimention getDatePhoneDimention() {
		return datePhoneDimention;
	}

	public void setDatePhoneDimention(DatePhoneDimention datePhoneDimention) {
		this.datePhoneDimention = datePhoneDimention;
	}

	public String getNetAddress() {
		return netAddress;
	}

	public void setNetAddress(String netAddress) {
		this.netAddress = netAddress;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}
	
	public void write(DataOutput out) throws IOException {
		datePhoneDimention.write(out);
		out.writeUTF(netAddress);
		out.writeInt(type);
	}

	public void readFields(DataInput in) throws IOException {
		this.datePhoneDimention.readFields(in);
		this.netAddress = in.readUTF();
		this.type = in.readInt();
	}

	public int compareTo(MobileDimention o) {
		if(this == o){
			return 0;
		}
		
		int tmp = datePhoneDimention.compareTo(o.datePhoneDimention);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = netAddress.compareTo(o.netAddress);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = type.compareTo(o.type);
		if(type != 0){
			return tmp;
		}
		return 0;
	}
	
	@Override
	public int hashCode() {
		int result = 1;
		int prime = 100;
		result = (result*prime) + (datePhoneDimention == null ? 0 : datePhoneDimention.hashCode());
		result = (result*prime) + (netAddress == null ? 0 : netAddress.hashCode());
		result = (result*prime) + (type == null ? 0 : type.hashCode());
		
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
		MobileDimention moDimention = (MobileDimention) obj;
		//========================判断日期和电话维度是否相同==============================
		if(this.datePhoneDimention == null){
			if(moDimention.datePhoneDimention != null){
				return false;
			}
		}else if(moDimention.datePhoneDimention == null){
			if(this.datePhoneDimention != null){
				return false;
			}
		}else if(!this.datePhoneDimention.equals(moDimention.datePhoneDimention)){
			return false;
		}
		//========================判断日期和电话维度是否相同==============================
		if(this.netAddress == null){
			if(moDimention.netAddress != null){
				return false;
			}
		}else if(moDimention.netAddress == null){
			if(this.netAddress != null){
				return false;
			}
		}else if(!this.netAddress.equals(moDimention.netAddress)){
			return false;
		}
		//========================判断日期和电话维度是否相同==============================
		if(this.type == null){
			if(moDimention.type != null){
				return false;
			}
		}else if(moDimention.type == null){
			if(this.type != null){
				return false;
			}
		}else if(!this.type.equals(moDimention.type)){
			return false;
		}	
		return true;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return datePhoneDimention.toString() + "\t" + netAddress + "\t" + type;
	}

}
