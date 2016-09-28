package into.puton.practice.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by taoyang on 2016/9/28.
 */
public class SpecializedQueries {

    public static void moreLikeThisQuery(Client client){
        QueryBuilder qb = QueryBuilders.moreLikeThisQuery()
                .like("maybe i should wake up")
                .minTermFreq(1)
                .maxQueryTerms(25);
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void templateQuery(Client client){
        Map<String, Object> template_params = new HashMap();
        template_params.put("param_level", 5);
        QueryBuilder qb = QueryBuilders.templateQuery(
                "level_template",
                ScriptService.ScriptType.FILE,
                template_params);
        SearchResponse sr = client.prepareSearch("others")
                .setTypes("comments")
                .setQuery(qb)                 // Query
                .execute()
                .actionGet();
        System.out.println(sr);
    }

    public static void scriptQuery(Client client){
        QueryBuilder qb = QueryBuilders.scriptQuery(
                new Script("doc['level'].value > 1")
        );
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

//        moreLikeThisQuery(client);

//        templateQuery(client);

//        scriptQuery(client);

    }

}
