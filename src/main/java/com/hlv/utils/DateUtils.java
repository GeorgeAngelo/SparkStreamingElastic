package com.hlv.utils;

import org.apache.spark.streaming.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * @Author: taojiang
 * @Email: taojiang@66law.com
 * @Date: 2018/8/7 14:00
 * @Description: TODO(用一句话描述该文件做什么)
 * @version: V1.0
 */
public class DateUtils {
    static SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
    public static long getLongTime(String date){
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        try {
            return simpleDateFormat.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }
    public static long getLongTime(String date1,String date2){
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        try {
            return simpleDateFormat.parse(date1).getTime()-simpleDateFormat.parse(date2).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static Date parseDate(String date) throws ParseException {
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return  simpleDateFormat.parse(date);
    }

    public static String getDateString(Time time,Long interval){
        Date date = new Date(time.milliseconds() - interval);
        return simpleDateFormat.format(date);
    }
}
