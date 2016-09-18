package into.puton.practice.elasticsearch;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by taoyang on 2016/9/9.
 */
public class JsonBuilder {

    public static void main(String[] args) {

        XContentBuilder builder = null;
        try {
            builder = jsonBuilder()
                    .startObject()
                    .field("user", "yang")
                    .field("postDate", new Date())
                    .field("message", "i love Elasticsearch")
                    .endObject();
            System.out.println(builder.string());
        } catch (IOException e) {
            e.printStackTrace();
        }



    }

}
