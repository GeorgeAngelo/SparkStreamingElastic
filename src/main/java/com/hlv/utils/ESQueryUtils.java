package com.hlv.utils;

import com.alibaba.fastjson.JSON;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.spark.streaming.Time;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * @Author: taojiang
 * @Email: taojiang@66law.com
 * @Date: 2018/8/10 10:25
 * @Description: TODO(用一句话描述该文件做什么)
 * @version: V1.0
 */
public class ESQueryUtils {
    public static Time getMaxTime(Time lastTime) throws ParseException {
        SimpleDateFormat date=new SimpleDateFormat("yyyy-MM-dd");
        date.setTimeZone(TimeZone.getTimeZone("GMT"));
        String todate = date.format(new Date(lastTime.milliseconds()));
        System.out.println("查询日期是："+todate); //测试
        RestClient client=RestClient.builder(new HttpHost("192.168.11.10",9200)).build();
        String method="POST";
        String endPoint="ngx-access-2018.07.20/accesslog/_search";
        String query="{\n" +
                "  \"size\": 0,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"range\": {\n" +
                "            \"@timestamp\": {\n" +
                "              \"gte\": \""+todate+"T00:00:00.000Z\",\n" +
                "              \"lte\": \""+todate+"T23:59:59.000Z\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"_source\": [\n" +
                "    \"@timestamp\"\n" +
                "  ],\n" +
                "  \"aggs\": {\n" +
                "    \"max_agg\": {\n" +
                "      \"max\": {\n" +
                "        \"field\": \"@timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Map<String,String> map=new HashMap<>();
        map.put("pretty", "true");
        String value="";
        try {
            HttpEntity entity = new NStringEntity(query);
            Header header = new BasicHeader("Content-Type", "application/json");
            Response response = client.performRequest(method, endPoint, map, entity, header);
            value = EntityUtils.toString(response.getEntity());
        }catch (Exception e){
            value="-1";
        }
        if("-1".equals(value)){
            return null;
        }else {
            String time = JSON.parseObject(value).getJSONObject("aggregations").getJSONObject("max_agg").getString("value_as_string");
            System.out.println(time); //测试
            long longTime = DateUtils.getLongTime(time);
            return new Time(longTime);
        }
    }
}
