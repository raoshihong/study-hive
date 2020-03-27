package com.rao.study.hive.udf;


import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.ArrayList;
import java.util.List;

public class MyUDF extends UDF {

    public Integer evaluate(Integer num){
        return num + 5;
    }

    /**
     * 支持重载
     * @param str
     * @return
     */
    public String evaluate(String str){
        return str.toUpperCase();
    }

    /**
     * 可以重载多个参数
     * @param num1
     * @param num2
     * @return
     */
    public Integer evaluate(int num1,int num2){
        return num1 + num2;
    }

    public List<String> evaluate(){
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        return list;
    }

}
