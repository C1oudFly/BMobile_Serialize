package com.oarcle.mobile.date.mobile.collector;

import java.sql.PreparedStatement;

import com.oarcle.mobile.phone.flow.mapper.dimention.FlowNetCountValue;
import com.oarcle.mobile.phone.flow.mapper.dimention.MobileDimention;

public interface BaseCollector {
	public void setPreparedStatement(PreparedStatement ps,MobileDimention key,FlowNetCountValue value);
}
