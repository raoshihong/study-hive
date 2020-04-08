package com.rao.study.hive.udtf;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

public class MultUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //定义转化后列的名称,可以转化为多列多行
        List<String> fieldNames = Lists.newArrayList();
        fieldNames.add("id");
        fieldNames.add("name");
        fieldNames.add("url");

        //定义转化后数据的类型
        List<ObjectInspector> valueTypes = Lists.newArrayList();
        //必须使用hive中提供的字符串类型
        valueTypes.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        valueTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        valueTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,valueTypes);
    }

    public void process(Object[] objects) throws HiveException {
        try {
            String json = String.valueOf(objects[0]);
            JSONArray jsonArray = new JSONArray(json);
            List<Object> list = Lists.newArrayList();
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                String name = jsonObject.getString("name");
                String url = jsonObject.getString("url");
                Integer id = jsonObject.getInt("id");

                list.clear();
                list.add(id);
                list.add(name);
            list.add(url);
                forward(list);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void close() throws HiveException {

    }
}
