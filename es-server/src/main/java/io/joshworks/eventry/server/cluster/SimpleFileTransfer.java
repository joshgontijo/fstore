//package io.joshworks.eventry.network;
//
//import io.joshworks.eventry.network.partition.FileHeader;
//import org.jgroups.JChannel;
//import org.jgroups.Message;
//import org.jgroups.ReceiverAdapter;
//import org.jgroups.conf.ClassConfigurator;
//
//import java.io.OutputStream;
//import java.util.Map;
//import java.util.Scanner;
//import java.util.concurrent.ConcurrentHashMap;
//
//
//public class SimpleFileTransfer extends ReceiverAdapter {
//    protected String filename;
//    protected JChannel channel;
//
//
//
//
//    public static void main(String[] args) throws Exception {
////        String props = "config.xml";
////        String filename = null;
////        String name = null;
////        for (int i = 0; i < args.length; i++) {
////            if (args[i].equals("-props")) {
////                props = args[++i];
////                continue;
////            }
////            if (args[i].equals("-name")) {
////                name = args[++i];
////                continue;
////            }
////            if (args[i].equals("-file")) {
////                filename = args[++i];
////                continue;
////            }
////            help();
////            return;
////        }
////        if (filename == null) {
////            help();
////            return;
////        }
//
//        new SimpleFileTransfer().start();
//    }
//
//
//    private void start() throws Exception {
//        ClassConfigurator.add(ID, FileHeader.class);
//        channel = new JChannel(Thread.currentThread().getContextClassLoader().getResourceAsStream("tcp.xml"));
//        channel.setReceiver(this);
//        channel.connect("FileCluster");
//        eventLoop();
//    }
//
//    private void eventLoop() throws Exception {
//        Scanner scanner = new Scanner(System.in);
//        while (true) {
//            System.out.print("File to be transfered: ");
//            filename = scanner.nextLine();
//            System.out.println();
//            sendFile();
//        }
//    }
//
//    protected void sendFile() throws Exception {
//
//    }
//
//
//    public void receive(Message msg) {
//
//    }
//
//
//
//
//
//    /*protected static Buffer readFile(String filename) throws Exception {
//        File file=new File(filename);
//        int size=(int)file.length();
//        FileInputStream input=new FileInputStream(file);
//        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(size);
//        byte[] read_buf=new byte[1024];
//        int bytes;
//        while((bytes=input.read(read_buf)) != -1)
//            out.write(read_buf, 0, bytes);
//        return out.getBuffer();
//    }*/
//
//
//
//
//}