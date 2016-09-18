package into.puton.practice.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by taoyang on 2016/9/11.
 */
public class DocumentAPI {

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

//        jsonIndex(client);

//        mapIndex(client);

//        delete(client);

//        update(client);

//        multiGet(client);

//        bulk(client);

        client.close();

    }

    public static void jsonIndex(Client client){

        try {
            IndexResponse response = client.prepareIndex("others", "comments","1")
                    .setSource(jsonBuilder()
                                    .startObject()
                                    .field("user", "yang")
                                    .field("message", "Bolin, if possible, I wish to be with you forever.")
                                    .field("level", 10)
                                    .field("postDate", new Date())
                                    .endObject()
                    )
                    .get();

            System.out.println(response.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            IndexResponse response = client.prepareIndex("others", "comments","3")
                    .setSource(jsonBuilder()
                                    .startObject()
                                    .field("user", "yang")
                                    .field("message", "Bolin, just smile.")
                                    .field("level", 8)
                                    .field("postDate", new Date())
                                    .endObject()
                    )
                    .get();

            System.out.println(response.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void mapIndex(Client client){

        Map json = new HashMap();

        json.put("user", "yang");
        json.put("message", "Bolin, wake up!");
        json.put("level", 5);
        json.put("postDate", new Date());

        IndexResponse response = client.prepareIndex("others", "comments","2")
                .setSource(json)
                .get();

        System.out.println(response.getId());

        Map json2 = new HashMap();

        json.put("user", "yang");
        json.put("message", "Bolin, bolin~");
        json.put("level", 5);
        json.put("postDate", new Date());

        IndexResponse response2 = client.prepareIndex("others", "comments","4")
                .setSource(json)
                .get();

        System.out.println(response2.getId());

    }

    public static void delete(Client client) {

        DeleteResponse response = client.prepareDelete("others", "comments", "3").get();

        System.out.println(response.getId());

    }

    public static void update(Client client) {

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("others");
        updateRequest.type("comments");
        updateRequest.id("1");
        try {
            updateRequest.doc(jsonBuilder()
                    .startObject()
                    .field("user", "goodman")
                    .field("message", "Bolin, wish you happiness.")
                    .field("level", 0)
                    .endObject());
            client.update(updateRequest).get();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    public static void multiGet(Client client) {

        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("others", "comments", "1")
                .add("others", "comments", "2", "3")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }

    }

    public static void bulk(Client client) {

        BulkRequestBuilder bulkRequest = client.prepareBulk();

// either use client#prepare, or use Requests# to directly build index/delete requests
        try {
            bulkRequest.add(client.prepareIndex("others", "comments", "1")
                            .setSource(jsonBuilder()
                                            .startObject()
                                            .field("user", "yang")
                                            .field("postDate", new Date())
                                            .field("message", "memeda")
                                            .endObject()
                            )
            );


            bulkRequest.add(client.prepareIndex("others", "comments", "2")
                            .setSource(jsonBuilder()
                                            .startObject()
                                            .field("user", "yang")
                                            .field("postDate", new Date())
                                            .field("message", "heheda")
                                            .endObject()
                            )
            );

            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
