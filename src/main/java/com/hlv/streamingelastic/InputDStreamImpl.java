package com.hlv.streamingelastic;

import com.hlv.utils.DateUtils;
import com.hlv.utils.ESQueryUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Option;
import scala.Some;
import scala.reflect.ClassTag;

import java.text.ParseException;


/**
 * @Author: taojiang
 * @Email: taojiang@66law.com
 * @Date: 2018/8/9 16:06
 * @Description: TODO(用一句话描述该文件做什么)
 * @version: V1.0
 */
public class InputDStreamImpl extends InputDStream<String>{
    private Time lastTime=new Time(DateUtils.getLongTime("2018-07-20T16:00:15.000Z"));
    private String resource="ngx-access-2018.07.20/accesslog/";
    private JavaStreamingContext jssc;
    private ClassTag evidence;
    private Long interval=15000l;
    public InputDStreamImpl(StreamingContext _ssc, ClassTag evidence$1) {
        super(_ssc, evidence$1);
        //this.jssc=new JavaStreamingContext(_ssc);
        this.evidence=evidence$1;
        this.interval=_ssc.graph().batchDuration().milliseconds();
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }


    @Override
    public Option<RDD<String>> compute(Time validTime) {
        Time maxTime=null;
        try {
            maxTime = ESQueryUtils.getMaxTime(lastTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if(maxTime!=null&&maxTime.greater(lastTime)) {
            String begin=DateUtils.getDateString(lastTime, interval);
            String end=DateUtils.getDateString(lastTime, 0l);
            System.out.println(begin+"-"+end);
            String query = "{\n" +
                    "  \"query\": {\n" +
                    "    \"bool\": {\n" +
                    "      \"must\": [\n" +
                    "        {\n" +
                    "          \"range\": {\n" +
                    "            \"@timestamp\": {\n" +
                    "              \"gte\": \""+begin+"\",\n" +
                    "              \"lte\": \""+end+"\"\n" +
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"exists\": {\n" +
                    "            \"field\": \"remote_addr\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ]\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
            RDD<String> rdd = JavaEsSpark.esJsonRDD(new JavaSparkContext(super.ssc().sparkContext()),
                    resource,
                    query).values().rdd();
            lastTime=new Time(lastTime.milliseconds()+interval);
            return new Some<>(rdd);
        }else {
            return (Option)Option.empty();
        }
    }
}
