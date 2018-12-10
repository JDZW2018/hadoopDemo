package cn.com.yusys;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import scala.Tuple2;

/**
 * @version 1.0.0
 * @项目名称: flink-parent
 * @类名称: org.apache.flink.streaming.examples.wiki
 * @类描述:
 * @功能描述:
 * @创建人: tianfs1@yusys.com.cn
 * @创建时间: 2018/12/10
 * @修改备注:
 * @修改记录: 修改时间    修改人员    修改原因
 * -------------------------------------------------------------
 * @Copyright (c) 2018宇信科技-版权所有
 */
public class WikipediaAnalysis {

    public static void main(String[] args)throws Exception {
        //创建一个streaming程序运行的上下文
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //source部分数据来源
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy((KeySelector<WikipediaEditEvent, String>) event -> {
            return event.getUser();
        });

        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))//指定窗口的宽度为5S
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String,Long>>() {

                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> stringLongTuple2, WikipediaEditEvent o) throws Exception {
                        System.out.println(o.toString());
                        return null;
                    }
                });
        result.print();
        see.execute();
    }
}
