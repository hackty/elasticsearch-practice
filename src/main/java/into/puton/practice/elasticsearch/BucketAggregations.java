package into.puton.practice.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by taoyang on 2016/9/26.
 */
public class BucketAggregations {

    public static void global(Client client){
        AggregationBuilder aggregation = AggregationBuilders
                .global("agg")
                .subAggregation(AggregationBuilders.terms("levels").field("level"));
        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();
        // sr is here your SearchResponse object
        Global agg = sr.getAggregations().get("agg");
        long count = agg.getDocCount(); // Doc count
        System.out.println(count);
    }

    public static void filter(Client client){
        AggregationBuilder aggregation = AggregationBuilders
                .filter("agg")
                .filter(QueryBuilders.termQuery("level", 5));
        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();
        // sr is here your SearchResponse object
        Filter agg = sr.getAggregations().get("agg");
        long count = agg.getDocCount(); // Doc count
        System.out.println(count);
    }

    public static void filters(Client client){
        AggregationBuilder aggregation =
                AggregationBuilders
                        .filters("agg")
                        .filter("middle", QueryBuilders.termQuery("level", 5))
                        .filter("high", QueryBuilders.termQuery("level", 8));
        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();
        // sr is here your SearchResponse object
        Filters agg = sr.getAggregations().get("agg");
        // For each entry
        for (Filters.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();            // bucket key
            long docCount = entry.getDocCount();            // Doc count
            System.out.println("key:" + key + " " + "docCount:" + docCount);
        }
    }

    public static void missing(Client client) {
        AggregationBuilder aggregation = AggregationBuilders.missing("agg").field("user");
        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();
        // sr is here your SearchResponse object
        Missing agg = sr.getAggregations().get("agg");
        long count = agg.getDocCount(); // Doc count
        System.out.println(count);
    }

    public static void terms(Client client){
        AggregationBuilder aggregation = AggregationBuilders
                .terms("levels")
                .field("level")
//                .order(Terms.Order.term(true));
                .order(Terms.Order.count(false));
        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();
        // sr is here your SearchResponse object
        Terms levels = sr.getAggregations().get("levels");

        // For each entry
        for (Terms.Bucket entry : levels.getBuckets()) {
            entry.getKey();      // Term
            System.out.println("key:" + entry.getKey());
            entry.getDocCount(); // Doc count
            System.out.println("count:" + entry.getDocCount());
        }
    }

    public static void range(Client client){

        AggregationBuilder aggregation =
                AggregationBuilders
                        .range("agg")
                        .field("level")
                        .addUnboundedTo(5)               // from -infinity to 1.0 (excluded)
                        .addRange(5, 8)               // from 1.0 to 1.5 (excluded)
                        .addUnboundedFrom(8);            // from 1.5 to +infinity

        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();

        // sr is here your SearchResponse object
        Range agg = sr.getAggregations().get("agg");

        // For each entry
        for (Range.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();             // Range as key
            Number from = (Number) entry.getFrom();          // Bucket from
            Number to = (Number) entry.getTo();              // Bucket to
            long docCount = entry.getDocCount();    // Doc count
            System.out.println("key " + key + ", from " + from + ", to " + to +  ", doc_count " + docCount);
        }

    }

    public static void significantTerms(Client client){
        AggregationBuilder aggregation =
                AggregationBuilders
                        .significantTerms("significant_users")
                        .field("user");
        // Let say you search for men only
        SearchResponse sr = client.prepareSearch("others")
                .setQuery(QueryBuilders.termQuery("level", 5))
                .addAggregation(aggregation)
                .get();

        // sr is here your SearchResponse object
        SignificantTerms agg = sr.getAggregations().get("significant_users");

        // For each entry
        for (SignificantTerms.Bucket entry : agg.getBuckets()) {
            entry.getKey();      // Term
            System.out.println(entry.getKey());
            entry.getDocCount(); // Doc count
            System.out.println(entry.getDocCount());
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

//        global(client);

//        filter(client);

//        missing(client);

//        terms(client);

//        significantTerms(client);

//        range(client);

    }

}
