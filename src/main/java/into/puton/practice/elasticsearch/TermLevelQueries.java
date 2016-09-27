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
public class TermLevelQueries {

    public static void termQuery(Client client){
        QueryBuilder qb = QueryBuilders.termQuery(
                "message",
                "wake"
        );
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void termsQuery(Client client){
        QueryBuilder qb = QueryBuilders.termsQuery(
                "message",
                "wake", "forever"
        );
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void rangeQuery(Client client){
        QueryBuilder qb = QueryBuilders.rangeQuery("level")
                .gt(6)
                .lt(9);
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void existsQuery(Client client){
        QueryBuilder qb = QueryBuilders.existsQuery("level");
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void missingQuery(Client client){
        QueryBuilder qb = QueryBuilders.missingQuery("nofield")
                .existence(true)
                .nullValue(true);
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void prefixQuery(Client client){
        QueryBuilder qb = QueryBuilders.prefixQuery(
                "message",
                "wak"
        );
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void wildcardQuery(Client client){
        QueryBuilder qb = QueryBuilders.wildcardQuery(
                "message",
                "fore*er"
        );
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void regexpQuery(Client client){
        QueryBuilder qb = QueryBuilders.regexpQuery(
                "message",
                "for.*er"
        );
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void fuzzyQuery(Client client){
        QueryBuilder qb = QueryBuilders.fuzzyQuery(
                "message",
                "wake"
        );
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void typeQuery(Client client){
        QueryBuilder qb = QueryBuilders.typeQuery("comments");
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void idsQuery(Client client){
        QueryBuilder qb = QueryBuilders.idsQuery("comments")
                .addIds("1","3");
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

//        termQuery(client);

//        termsQuery(client);

//        rangeQuery(client);

//        existsQuery(client);

//        missingQuery(client);

//        prefixQuery(client);

//        wildcardQuery(client);

//        regexpQuery(client);

//        fuzzyQuery(client);

//        typeQuery(client);

//        idsQuery(client);

    }
}

