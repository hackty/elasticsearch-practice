package into.puton.practice.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by taoyang on 2016/9/9.
 */
public class SniffClient {

    public static void main(String[] args) {
        Client client = null;

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true)
                .build();
        client = TransportClient.builder().settings(settings).build();

//        on shutdown
        client.close();
    }

}
