package io.joshworks.fstore.tools.shell;

import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.restclient.http.RestClient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Scanner;

public class Shell {

    private final RestClient client;

    public Shell(String server) {
        this.client = RestClient.builder().baseUrl(server).build();
    }


    public static void main(String[] args) {

        String server = args[0];
        if(StringUtils.isBlank(server)) {
            throw new IllegalArgumentException("server must be provided");
        }

        Shell shell = new Shell(server);






    }


    private static class Input implements Runnable {

        private final OutputStream out;
        private final Scanner scanner;

        private Input(OutputStream out) {
            this.out = out;
            this.scanner = new Scanner(System.in);
        }

        @Override
        public void run() {
            try {
                while(true) {
                    String line = scanner.nextLine();
                    processCommand(line);

                }

                final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
                String line;
                while ((line = writer.readLine()) != null) {
                    System.out.println(line);
                }
                writer.close();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        }

        private void processCommand(String line) {

        }
    }

    private static class Output implements Runnable {

        private final InputStream in;

        private Output(InputStream in) {
            this.in = in;
        }

        @Override
        public void run() {
            try {
                final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
                reader.close();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        }
    }

}


