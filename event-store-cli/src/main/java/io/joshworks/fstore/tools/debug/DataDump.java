package io.joshworks.fstore.tools.debug;

import io.joshworks.restclient.http.HttpResponse;
import io.joshworks.restclient.http.MediaType;
import io.joshworks.restclient.http.Unirest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DataDump {

    public static void main(String[] args) throws IOException {

        try(BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\jgontijo\\Projects\\next-best-offer\\clickstream-dump.txt"))) {
            String line;
            while((line = reader.readLine()) != null) {
            HttpResponse<String> response = Unirest.post("http://localhost:9000/streams/clickstream")
                    .contentType(MediaType.APPLICATION_JSON_TYPE)
                    .body(line)
                    .asString();

                if (!response.isSuccessful()) {
                    System.err.println(response.getBody());
                }
            }
        }

    }

}
