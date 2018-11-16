package com.hlv.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hlv.utils.InputDStreamImpl;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.FileWriter;
import java.util.List;

/**
 * @Author: taojiang
 * @Email: taojiang@66law.com
 * @Date: 2018/8/9 16:32
 * @Description: TODO(用一句话描述该文件做什么)
 * @version: V1.0
 */
public class StreamingTest {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf()
                .setAppName("StreamingTest")
                .setMaster("local[*]")
                .set("es.nodes", "192.168.11.10")
                .set("es.port", "9200")
                .set("es.scroll.size", "10000");

        //System.setProperty("hadoop.home.dir", System.getProperty("user.dir")+"\\conf");

        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(15));
        jsc.checkpoint("F:\\checkPoint\\");

        ClassTag<String> tag= ClassTag$.MODULE$.apply(String.class);

        InputDStreamImpl in = new InputDStreamImpl(jsc.ssc(), tag);

        JavaInputDStream<String> inputDStream=new JavaInputDStream(in, tag);



        JavaPairDStream<String, Long> pairDStream = inputDStream.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                JSONObject object = JSON.parseObject(s);
                String ipAddress = object.getString("remote_addr");
                return new Tuple2<>(ipAddress, 1L);
            }
        });

        JavaPairDStream<String, Long> res = pairDStream.transformToPair(new Function<JavaPairRDD<String, Long>, JavaPairRDD<String, Long>>() {

            @Override
            public JavaPairRDD<String, Long> call(JavaPairRDD<String, Long> rdd) throws Exception {
                return  rdd.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long aLong, Long aLong2) throws Exception {
                        return aLong + aLong2;
                    }
                }).mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Long> t) throws Exception {
                        return new Tuple2<>(t._2, t._1);
                    }
                }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Long, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<Long, String> t) throws Exception {
                        return new Tuple2<>(t._2, t._1);
                    }
                });


            }
        });


        JavaPairDStream<String, Long> dStream = res.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {


            @Override
            public Optional<Long> call(List<Long> v1, Optional<Long> v2) throws Exception {
                Long sum=0l;
                if(v2.isPresent()){
                    sum=v2.get();
                }
                for(Long value:v1){
                    sum+=value;
                }
                return Optional.of(sum);
            }
        });


        dStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                JavaPairRDD<String, Long> rdd1 = rdd.sortByKey(false);
                List<Tuple2<String, Long>> take = rdd1.take(10);
                for(Tuple2<String, Long> t:take){
                    System.out.println(t._1+"----"+t._2);
                }
                rdd1.saveAsTextFile("F:\\log");
            }
        });


        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
