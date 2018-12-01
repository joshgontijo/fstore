package io.joshworks.fstore.tools.shell;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Shell implements Runnable {


    public Shell(String server) {
    }


    public static void main(String[] args) {
        Shell shell = new Shell(null);
        shell.run();

    }

    @Override
    public void run() {
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String command;
            while ((command = reader.readLine()) != null) {
                System.out.println(command);
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }


}


