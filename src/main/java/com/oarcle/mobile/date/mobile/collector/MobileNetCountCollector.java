package com.oarcle.mobile.date.mobile.collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oarcle.mobile.phone.flow.mapper.dimention.FlowNetCountValue;
import com.oarcle.mobile.phone.flow.mapper.dimention.MobileDimention;

/**
 * 为网站统计次数
 * @author Administrator
 *
 */
public class MobileNetCountCollector implements BaseCollector {

	public void setPreparedStatement(PreparedStatement ps, MobileDimention key, FlowNetCountValue value) {
		try {
			ps.setString(1, key.getDatePhoneDimention().getPhoneDate());
			ps.setString(2, key.getDatePhoneDimention().getPhoneNumber());
			ps.setString(3, String.valueOf(value.getCount()));
			ps.setString(4, String.valueOf(value.getCount()));
			
			ps.addBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
