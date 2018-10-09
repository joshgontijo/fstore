package io.joshworks.eventry.it;

import io.joshworks.restclient.http.HttpResponse;
import io.joshworks.restclient.http.RestClient;
import org.json.JSONObject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EsClient {

    public static final ExecutorService executor = Executors.newFixedThreadPool(100);

    public static void main(String[] args) {

        for (int j = 0; j < 100; j++) {
            executor.submit(() -> {
                RestClient client = RestClient.builder().baseUrl("http://localhost:9000").build();

                JSONObject object = new JSONObject();
                JSONObject data = new JSONObject();
                data.put("name", "Josh");
                data.put("age", 2);

                object.put("type", "type");
                object.put("data", data);

                for (int i = 0; i < 10000; i++) {

                    HttpResponse<String> response = client.get("/ping")
//                            .contentType(MediaType.APPLICATION_JSON_TYPE)
//                            .body(object)
                            .asString();
                    if(!response.isSuccessful()) {
                        System.err.println("Error: " + response.getBody());
                    }
                }

            });
        }




    }

}
