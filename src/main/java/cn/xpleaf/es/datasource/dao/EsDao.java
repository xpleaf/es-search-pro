package cn.xpleaf.es.datasource.dao;

import cn.xpleaf.es.datasource.utils.EsUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Leaf
 * @date 2018/9/4 下午11:28
 */
public class EsDao {

    /**
     * 获取mysql连接
     */
    private static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String user = "root";
            String password = "root";
            String url = "jdbc:mysql://localhost:3306/News";
            conn = DriverManager.getConnection(url, user, password);
            if(conn != null) {
                System.out.println("mysql连接成功！");
            } else {
                System.out.println("mysql连接失败！");
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 从mysql导入数据到es
     */
    public static void mysql2Es() {
        Connection conn = getConnection();
        String sql = "SELECT * FROM news";
        TransportClient client = EsUtils.getSingleClient();
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            Map<String, Object> map = new HashMap<String, Object>();
            while (resultSet.next()) {
                int nid = resultSet.getInt(1);
                map.put("id", nid);
                map.put("title", resultSet.getString(2));
                map.put("key_word", resultSet.getString(3));
                map.put("content", resultSet.getString(4));
                map.put("url", resultSet.getString(5));
                map.put("reply", resultSet.getString(6));
                map.put("source", resultSet.getString(7));
                String postDatetime = resultSet.getTimestamp(8).toString();
                map.put("postdate", postDatetime.substring(0, postDatetime.length() - 2));
                System.out.println(map);
                IndexResponse indexResponse = client.prepareIndex("spnews", "news", String.valueOf(nid))
                        .setSource(map)
                        .execute().actionGet();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
