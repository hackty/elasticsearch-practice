package into.puton.practice.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by taoyang on 2016/9/28.
 */
public class IndicesAdministration {

    public static void createIndex(Client client){
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        client.admin().indices().prepareCreate("myindex1").get();
    }

    public static void putMapping(Client client){
        client.admin().indices().prepareCreate("myindex2")
                .addMapping("mytype1", "{\n" +
                        "    \"mytype1\": {\n" +
                        "      \"properties\": {\n" +
                        "        \"field1\": {\n" +
                        "          \"type\": \"string\",\n" +
                        "          \"analyzer\": \"ik\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }")
                .get();
    }

    public static void deleteMapping(Client client){
        client.admin().indices().prepareDelete("myindex2").get();
    }

    public static void main(String[] args) {

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "elasticsearch1")
                .build();

        TransportClient client = null;
        try {
            client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("tdh-master"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

//        createIndex(client);

//        putMapping(client);

//        deleteMapping(client);

    }

}
