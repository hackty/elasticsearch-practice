package into.puton.practice.elasticsearch;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * Created by taoyang on 2016/9/11.
 */
public class SearchAPI {

    public static void search(Client client){

        SearchResponse response = client.prepareSearch("others")
                .setTypes("comments")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("message", "bolin"))                 // Query
//                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();

        System.out.println(response.toString());

    }

    public static void scroll(Client client){

        QueryBuilder qb = termQuery("message", "bolin");

        SearchResponse scrollResp = client.prepareSearch("others")
                .setTypes("comments")
                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                .setSize(100).execute().actionGet(); //100 hits per shard will be returned for each scroll
//Scroll until no hits are returned
        while (true) {

            for (SearchHit hit : scrollResp.getHits().getHits()) {

                System.out.println(hit.getSource().get("message"));
                //Handle the hit...
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }

    }

    public static void multiSearch(Client client){

        SearchRequestBuilder srb1 = client
                .prepareSearch().setQuery(QueryBuilders.queryStringQuery("heheda")).setSize(1);
        SearchRequestBuilder srb2 = client
                .prepareSearch().setQuery(QueryBuilders.matchQuery("message", "bolin")).setSize(1);

        MultiSearchResponse sr = client.prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .execute().actionGet();

        // You will get all individual responses from MultiSearchResponse#getResponses()
        long nbHits = 0;
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();
            nbHits += response.getHits().getTotalHits();
            System.out.println(response.getHits().getAt(0).getSource());
        }

    }

    public static void aggregations(Client client){

        SearchResponse sr = client.prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.terms("agg1").field("user")
                )
//                .addAggregation(
//                        AggregationBuilders.dateHistogram("agg2")
//                                .field("message")
//                                .interval(DateHistogramInterval.YEAR)
//                )
                .execute().actionGet();

        // Get your facet results
        Terms agg1 = sr.getAggregations().get("agg1");
        DateHistogramInterval agg2 = sr.getAggregations().get("agg2");

        System.out.println(agg1.getBuckets().get(0).getKey());

    }

    public static void terminateAfter(Client client){
        SearchResponse sr = client.prepareSearch("others")
                .setTerminateAfter(1)
                .get();

        if (sr.isTerminatedEarly()) {
            System.out.println("TerminatedEarly");
            // We finished early
        }
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

//        search(client);

//        scroll(client);

//        multiSearch(client);

//        aggregations(client);

        terminateAfter(client);

    }

}
