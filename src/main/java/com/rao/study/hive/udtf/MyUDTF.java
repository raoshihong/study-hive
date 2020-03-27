package com.rao.study.hive.udtf;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.List;

public class MyUDTF extends GenericUDTF {

    /**
     * 初始化方法中定义列名和数据类型
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //定义转化后列的名称,可以转化为多列多行
        List<String> fieldNames = Lists.newArrayList();
        fieldNames.add("name");

        //定义转化后数据的类型
        List<ObjectInspector> valueTypes = Lists.newArrayList();
        //必须使用hive中提供的字符串类型
        valueTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,valueTypes);
    }

    /**
     * 处理每行的数据,objects为自定义函数的参数
     * @param objects
     * @throws HiveException
     */
    public void process(Object[] objects) throws HiveException {
        //比如将字符串拆分,实现explode函数的功能
        String str = objects[0].toString();
        String separator = objects[1].toString();
        String[] names = str.split(separator);
        List<String> nameList = Lists.newArrayList();

        for(String name:names) {
            //调用父类的forward方法,将数据输出到收集器中
            nameList.clear();
            nameList.add(name);
            forward(nameList);
        }
    }

    public void close() throws HiveException {

    }
}
