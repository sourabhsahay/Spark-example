package com.expedia.www.spark;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.joda.time.Days;
import org.joda.time.LocalDate;

public class GenerateDirectoryPaths {

	/*
	 * Needed for getting last 30 days when it spans across two months
	 * 
	 */
	public static void main(String[] args) {
		String basePath = " s3://userinteractionmessages/raw/prod/";
		String separator = "/";
		DecimalFormat df = new DecimalFormat("00");
		List<String>outputPaths = new ArrayList<String>();
		String pathSeparator ="/";
		Date today = new Date();
		System.out.println(today);
		Calendar cal = new GregorianCalendar();
		cal.setTime(today);
		cal.add(Calendar.DAY_OF_MONTH, -1);
		Date today30 = cal.getTime();
		System.out.println(today30);
		LocalDate startDate = LocalDate.fromDateFields(today30);
		int days = Days.daysBetween(startDate,LocalDate.fromDateFields(today)).getDays()+1;
		for (int i=0; i < days; i++) {
		    LocalDate localeDate = startDate.plusDays(i);
		    outputPaths.add(basePath + localeDate.getYearOfEra() +separator
		               +  df.format(localeDate.getMonthOfYear()) + separator +  df.format(localeDate.getDayOfMonth())+ separator +"**/*");
		}
		System.out.println(StringUtils.join(outputPaths,","));
		
	}

}
