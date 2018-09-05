package cn.xpleaf.es.datasource;

import cn.xpleaf.es.datasource.dao.EsDao;
import cn.xpleaf.es.datasource.utils.EsUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * @author Leaf
 * @date 2018/9/4 下午11:50
 */
public class ImportData {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        // 1.创建索引
        EsUtils.createIndex("spnews", 3, 0);
        // 2.设置Mapping
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject().startObject("properties")
                    .startObject("id")
                    .field("type", "long")
                    .endObject()
                    .startObject("title")
                    .field("type", "text")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word")
                    .endObject()
                    .startObject("key_word")
                    .field("type", "text")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word")
                    .endObject()
                    .startObject("content")
                    .field("type", "text")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word")
                    .endObject()
                    .startObject("url")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("reply")
                    .field("type", "long")
                    .endObject()
                    .startObject("source")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("postdate")
                    .field("type", "date")
                    .field("format", "yyyy-MM-dd HH:mm:ss")
                    .endObject()
                    .endObject()
                    .endObject();
            EsUtils.setMapping("spnews", "news", builder.string());
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 3.读取mysql数据到es
        EsDao.mysql2Es();

        long end = System.currentTimeMillis();
        System.out.println(String.format("消耗时间：%s ms" , end - start));
    }

}

/**
 * 分析
 *
 * 消耗时间：22020 ms
 * 记录总数为：5570条
 * 每条记录字段数：8
 * ES中记录的索引库大小：33.9M
 * 在mysql中查询数据的大小：
 * 要查询表所占的容量，就是把表的数据和索引加起来就可以了
 * mysql> select concat(round(sum(DATA_LENGTH/1024/1024),2),'M') from information_schema.tables
 * where table_schema='news' AND table_name='news';
 * +-------------------------------------------------+
 * | concat(round(sum(DATA_LENGTH/1024/1024),2),'M') |
 * +-------------------------------------------------+
 * | 22.55M                                          |
 * +-------------------------------------------------+
 * 1 row in set (0.00 sec)
 * 数据量大小会比es小，原因很简单，因为其没有做索引
 */
