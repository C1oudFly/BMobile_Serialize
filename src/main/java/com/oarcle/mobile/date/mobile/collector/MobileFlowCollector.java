package com.oarcle.mobile.date.mobile.collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oarcle.mobile.phone.flow.mapper.dimention.FlowNetCountValue;
import com.oarcle.mobile.phone.flow.mapper.dimention.MobileDimention;

public class MobileFlowCollector implements BaseCollector {

	public void setPreparedStatement(PreparedStatement ps, MobileDimention key, FlowNetCountValue value) {
		try {
			ps.setString(1, key.getDatePhoneDimention().getPhoneDate());
			ps.setString(2, key.getDatePhoneDimention().getPhoneNumber());
			ps.setString(3, String.valueOf(value.getUpFlow()));
			ps.setString(4, String.valueOf(value.getDownFlow()));
			ps.setString(5, String.valueOf(value.getTotalFlow()));
			ps.setString(6, String.valueOf(value.getUpFlow()));
			ps.setString(7, String.valueOf(value.getDownFlow()));
			ps.setString(8, String.valueOf(value.getTotalFlow()));
			
			ps.addBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
