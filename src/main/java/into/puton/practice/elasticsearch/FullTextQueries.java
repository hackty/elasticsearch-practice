package into.puton.practice.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by taoyang on 2016/9/27.
 */
public class FullTextQueries {

    public static void matchQuery(Client client){
        QueryBuilder qb = QueryBuilders.matchQuery(
                "message",
                "i wake up"
        );
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void multiMatchQuery(Client client){
        QueryBuilder qb = QueryBuilders.multiMatchQuery(
                "smile",
                "message", "user"
        );
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void commonTermsQuery(Client client){
        QueryBuilder qb = QueryBuilders.commonTermsQuery(
                "message",
                "i wake up"
        );
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void queryStringQuery(Client client){
        QueryBuilder qb = QueryBuilders.queryStringQuery("+Bolin -forever");
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void simpleQueryStringQuery(Client client){
        QueryBuilder qb = QueryBuilders.simpleQueryStringQuery("+Bolin -forever");
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
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

//        matchQuery(client);

//        multiMatchQuery(client);

//        commonTermsQuery(client);

//        queryStringQuery(client);

//        simpleQueryStringQuery(client);

    }

}
