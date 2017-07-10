package com.oarcle.mobile.phone.flow.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.oarcle.mobile.phone.flow.constants.DateType;

public class DateUtils {
	
	public static String toDate(String times,int type){
		String value = "";
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(Long.parseLong(times.trim()));
		switch(type){
			case DateType.DATE :
				
				SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
				value = simpleDateFormat1.format(new Date(Long.parseLong(times.trim())));
				break;
			
			case DateType.MONTH:
				
				SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyy-MM");
				value = simpleDateFormat2.format(new Date(Long.parseLong(times.trim())));
				break;
			
			case DateType.YEAR:
				
				value = String.valueOf(calendar.get(Calendar.YEAR));
				break;
			
			case DateType.SEASON:
				
				int season = 0;
				int month = calendar.get(Calendar.MONTH) + 1;
				if(month % 3 == 0){
					season = month /3;
				}else {
					season = month /3 + 1;
				}
				int year = calendar.get(Calendar.YEAR);
				value = String.valueOf(year + "-" + season);
				break;
			
			default:
				
				throw new RuntimeException("没有你要转换的类型!!!");
				
		}
		return value;
	}
}
