package into.puton.practice.elasticsearch;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by taoyang on 2016/9/13.
 */
public class CountAPI {

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

        CountResponse response = client.prepareCount("others")
                .setQuery(termQuery("_type", "comments"))
                .execute()
                .actionGet();

        System.out.println(response.getCount());
    }

}
