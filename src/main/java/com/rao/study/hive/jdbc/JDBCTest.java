package com.rao.study.hive.jdbc;

import org.apache.hive.jdbc.HiveDriver;
import org.junit.Test;

import java.sql.*;

public class JDBCTest {

    @Test
    public void test() throws Exception{

        String url = "jdbc:hive2://hadoop102:10000";//连接地址,hadoop102是hive服务所在的服务器host
        String username = "root";
        String password = "123";
        Class.forName(HiveDriver.class.getName());
        Connection connection = DriverManager.getConnection(url,username,password);
        PreparedStatement pstmt = connection.prepareStatement("select videoid,age from gulivideo_ori limit 10");
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()){
            String videoid = rs.getString("videoid");
            String age = rs.getString("age");

            System.out.println("videoid:"+videoid+",age:"+age);
        }
    }

}
