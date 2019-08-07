package ES;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * Created by 向瑞祥 on 2019-08-07.
 */
public class InitDemo {

    public static RestHighLevelClient getClient() {


        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("104.196.146.203", 9200, "http")));
        return client;
    }
}
