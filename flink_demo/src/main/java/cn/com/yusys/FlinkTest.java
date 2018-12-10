package cn.com.yusys;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @version 1.0.0
 * @项目名称: hadoopDemo
 * @类名称: cn.com.yusys
 * @类描述:
 * @功能描述:
 * @创建人: tianfs1@yusys.com.cn
 * @创建时间: 2018/12/7
 * @修改备注:
 * @修改记录: 修改时间    修改人员    修改原因
 * -------------------------------------------------------------
 * @Copyright (c) 2018宇信科技-版权所有
 */
public class FlinkTest {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> data = env.readTextFile("C:\\Users\\94946\\Downloads\\HDFSTest.java");

        data
                .filter(new FilterFunction<String>() {
                    public boolean filter(String value) {
                        return value.startsWith("http://");
                    }
                })
                .writeAsText("C:\\Users\\94946\\Downloads\\HDFSTest.txt");

        JobExecutionResult res = env.execute();
    }


}
