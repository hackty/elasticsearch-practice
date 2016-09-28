package into.puton.practice.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by taoyang on 2016/9/27.
 */
public class CompoundQueries {

    public static void constantScoreQuery(Client client){
        QueryBuilder qb = QueryBuilders.constantScoreQuery(
                QueryBuilders.termQuery("message","wake"))
                .boost(2.0f);
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void boolQuery(Client client){
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("message", "bolin"))
                .mustNot(QueryBuilders.termQuery("message", "wake"))
                .should(QueryBuilders.termQuery("message", "smile"));
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void disMaxQuery(Client client){
        QueryBuilder qb = QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery("message", "bolin"))
                .boost(1.2f)
                .tieBreaker(0.7f);
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void functionScoreQuery(Client client){
        QueryBuilder qb = QueryBuilders.functionScoreQuery()
                .add(
                        QueryBuilders.matchQuery("message", "wake"),
                        ScoreFunctionBuilders.randomFunction("ABCDEF")
                )
                .add(
                        ScoreFunctionBuilders.exponentialDecayFunction("level", 0L, 1L)
                );
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void boostingQuery(Client client){
        QueryBuilder qb = QueryBuilders.boostingQuery()
                .positive(QueryBuilders.termQuery("message", "wake"))
                .negative(QueryBuilders.termQuery("message", "up"))
                .negativeBoost(0.2f);
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void indicesQuery(Client client){
        QueryBuilder qb = QueryBuilders.indicesQuery(
                QueryBuilders.termQuery("message", "nonono"),
                "others"
        ).noMatchQuery(QueryBuilders.termQuery("othername", "other value"));
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

//        constantScoreQuery(client);

//        boolQuery(client);

//        disMaxQuery(client);

//        functionScoreQuery(client);

//        boostingQuery(client);

//        indicesQuery(client);

    }

}
