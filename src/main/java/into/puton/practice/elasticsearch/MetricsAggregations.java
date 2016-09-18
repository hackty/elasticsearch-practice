package into.puton.practice.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by taoyang on 2016/9/13.
 */
public class MetricsAggregations {

    public static void min(Client client){

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .min("agg")
                        .field("level");

        SearchResponse sr = client.prepareSearch()
                .addAggregation(aggregation)
                .execute().actionGet();

        Min agg = sr.getAggregations().get("agg");
        double value = agg.getValue();

        System.out.println(value);

    }

    public static void max(Client client){

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .max("agg")
                        .field("level");

        SearchResponse sr = client.prepareSearch()
                .addAggregation(aggregation)
                .execute().actionGet();

        Max agg = sr.getAggregations().get("agg");
        double value = agg.getValue();

        System.out.println(value);

    }

    public static void sum(Client client){

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .sum("agg")
                        .field("level");

        SearchResponse sr = client.prepareSearch()
                .addAggregation(aggregation)
                .execute().actionGet();

        Sum agg = sr.getAggregations().get("agg");
        double value = agg.getValue();

        System.out.println(value);

    }

    public static void avg(Client client){

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .avg("agg")
                        .field("level");

        SearchResponse sr = client.prepareSearch()
                .addAggregation(aggregation)
                .execute().actionGet();

        Avg agg = sr.getAggregations().get("agg");
        double value = agg.getValue();

        System.out.println(value);

    }

    public static void stats(Client client){

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .stats("agg")
                        .field("level");

        SearchResponse sr = client.prepareSearch()
                .addAggregation(aggregation)
                .execute().actionGet();

        Stats agg = sr.getAggregations().get("agg");
        Double min = agg.getMin();
        double max = agg.getMax();
        double avg = agg.getAvg();
        double sum = agg.getSum();
        long count = agg.getCount();

        System.out.println(count);

    }

    public static void extendedStats(Client client){

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .extendedStats("agg")
                        .field("level");

        SearchResponse sr = client.prepareSearch()
                .addAggregation(aggregation)
                .execute().actionGet();

        ExtendedStats agg = sr.getAggregations().get("agg");
        Double min = agg.getMin();
        double max = agg.getMax();
        double avg = agg.getAvg();
        double sum = agg.getSum();
        long count = agg.getCount();

        double stdDeviation = agg.getStdDeviation();
        double sumOfSquares = agg.getSumOfSquares();
        double variance = agg.getVariance();

        System.out.println(stdDeviation);

    }

    public static void count(Client client){

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .count("agg")
                        .field("level");

        SearchResponse sr = client.prepareSearch()
                .addAggregation(aggregation)
                .execute().actionGet();

        ValueCount agg = sr.getAggregations().get("agg");
        double value = agg.getValue();

        System.out.println(value);

    }

    public static void percentiles(Client client){

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .percentiles("agg")
                        .field("level")
                        .percentiles(1.0, 5.0, 30.0, 50.0, 99.0, 100.0);

        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();

        // sr is here your SearchResponse object
        Percentiles agg = sr.getAggregations().get("agg");
        // For each entry
        for (Percentile entry : agg) {
            double percent = entry.getPercent();    // Percent
            double value = entry.getValue();        // Value

            System.out.println("percent:" + percent + " value:" + value);
        }

    }

    public static void percentileRanks(Client client){

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .percentileRanks("agg")
                        .field("level")
                        .percentiles(1, 5, 6, 10, 12);

        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();

        // sr is here your SearchResponse object
        PercentileRanks agg = sr.getAggregations().get("agg");
        // For each entry
        for (Percentile entry : agg) {
            double percent = entry.getPercent();    // Percent
            double value = entry.getValue();        // Value

            System.out.println("percent:" + percent + " value:" +  value );
        }

    }

    public static void cardinality(Client client){

        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .cardinality("agg")
                        .field("level");

        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();

        Cardinality agg = sr.getAggregations().get("agg");
        double value = agg.getValue();

        System.out.println(value);

    }

    public static void topHits(Client client){

        AggregationBuilder aggregation =
                AggregationBuilders
                        .terms("agg")
                        .field("level")
                        .subAggregation(
                                AggregationBuilders
                                        .topHits("top")
                                        .setExplain(true)
                                        .setSize(15)
                                        .setFrom(0)
                        );

        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();

        // sr is here your SearchResponse object
        Terms agg = sr.getAggregations().get("agg");

        // For each entry
        for (Terms.Bucket entry : agg.getBuckets()) {
            String key = String.valueOf(entry.getKey());                    // bucket key
            long docCount = entry.getDocCount();            // Doc count
            System.out.println("key:" + key + " docCount:" + docCount);

            // We ask for top_hits for each bucket
            TopHits topHits = entry.getAggregations().get("top");
            for (SearchHit hit : topHits.getHits().getHits()) {
                System.out.println("id:" + hit.getId() + " source:" + hit.getSourceAsString());
            }
        }

    }

    public static void scriptedMetric(Client client){
        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .scriptedMetric("agg")
                        .initScript(new Script("_agg['possibility'] = []"))
                        .mapScript(new Script("if (doc['level'].value >= 8 ) " +
                                "{ _agg.possibility.add(1) } " +
                                "else " +
                                "{ _agg.possibility.add(-1}"));

        SearchResponse sr = client.prepareSearch("others")
                .addAggregation(aggregation)
                .execute().actionGet();

        // sr is here your SearchResponse object
        ScriptedMetric agg = sr.getAggregations().get("agg");
        Object scriptedResult = agg.aggregation();
        System.out.println("scriptedResult:" + scriptedResult);

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

//        min(client);

//        max(client);

//        sum(client);

//        avg(client);

//        stats(client);

//        extendedStats(client);

//        count(client);

//        percentiles(client);

        //TODO I’m confused.
//        percentileRanks(client);

//        cardinality(client);

//        topHits(client);

        scriptedMetric(client);

    }

}
