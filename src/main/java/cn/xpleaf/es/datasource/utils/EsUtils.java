package cn.xpleaf.es.datasource.utils;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Leaf
 * @date 2018/9/4 下午11:02
 */
public class EsUtils {

    private final static String CLUSTER_NAME = "elasticsearch";
    private static final String HOST = "localhost";
    private static final int PORT = 9300;
    private static volatile TransportClient client;
    private static Settings settings = Settings.builder()
            .put("cluster.name", CLUSTER_NAME)
            .build();

    /**
     * 获取单例的TransportClient对象
     */
    public static TransportClient getSingleClient() {
        if(client == null) {
            synchronized (EsUtils.class) {
                if (client == null) {
                    try {
                        client = new PreBuiltTransportClient(settings)
                                .addTransportAddress(new InetSocketTransportAddress(
                                        InetAddress.getByName(HOST), PORT));
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return client;
    }

    /**
     * 获取操作索引的IndicesAdminClient对象
     */
    public static IndicesAdminClient getAdminClient() {
        return getSingleClient().admin().indices();
    }

    /**
     * 创建索引
     */
    public static boolean createIndex(String indexNmae, int shards, int replicas) {
        Settings settings = Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
                .build();
        CreateIndexResponse createIndexResponse = getAdminClient()
                .prepareCreate(indexNmae.toLowerCase())
                .setSettings(settings)
                .execute().actionGet();
        boolean isIndexCreated = createIndexResponse.isAcknowledged();
        if(isIndexCreated) {
            System.out.println("索引" + indexNmae + "创建成功");
        } else {
            System.out.println("索引" + indexNmae + "创建失败");
        }
        return isIndexCreated;
    }

    /**
     * 设置映射
     */
    public static boolean setMapping(String indexName, String typeName, String mapping) {
        PutMappingResponse putMappingResponse = getAdminClient()
                .preparePutMapping(indexName)
                .setType(typeName)
                .setSource(mapping, XContentType.JSON)
                .get();
        return putMappingResponse.isAcknowledged();
    }
}
