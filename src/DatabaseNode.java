/* -----------------------------------------------------------------
 * Klasa węzła dla projektu "Rozproszona baza danych".
 * The node class for the "A distributed database" project.
 *
 * Kompilacja/Compilation:
 * javac DatabaseNode.java
 * Uruchomienie/Execution:
 * java DatabaseNode -tcpport <TCP port number> -record <klucz>:<wartość>
 *      [ -connect <adres>:<port>]
 *
 * SKJ, 2022/23, Maciej Hołubczat
 */

import java.net.*;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class DatabaseNode {
    public static void main(String[] args) throws IOException {

        // parameter storage
        int port = 0;
        int key;
        int value;
        Map<String, Integer> nodes = new HashMap<String, Integer>() {
        };

        // Parameter scan loop
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-tcpport" -> port = Integer.parseInt(args[++i]);
                case "-record" -> {
                    String[] record = args[++i].split(":");
                    key = Integer.parseInt(record[0]);
                    value = Integer.parseInt(record[1]);
                }
                case "-connect" -> {
                    String[] connect = args[++i].split(":");
                    nodes.put(connect[0], Integer.parseInt(connect[1]));
                }
            }
        }

        // communication socket and streams
        Socket serverSocket;
        PrintWriter out;
        BufferedReader in;
        try {
            System.out.println("Open connection port " + port);
            serverSocket = new ServerSocket(port).accept();
            out = new PrintWriter(serverSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
            System.out.println("Connected");
            System.out.println("Receiving: ");
            String command = in.readLine();
            out.println(command);

            // process and handle command
            String[] commands = command.split(" ");
            for (int i = 0; i < commands.length; i++) {
                switch (commands[i]) {
                    case "set-value" -> {
                        String[] setValue = commands[++i].split(":");
                        //TODO znajdz node i ustaw node
                        int setKey = Integer.parseInt(setValue[0]);
                        value = Integer.parseInt(setValue[1]);
                    }
                    case "get-value" -> {
                        String getValue = commands[++i];
                        System.out.println("key value or error if no base");
                    }
                    case "find-key" -> {
                        String findKey = commands[++i];
                        System.out.println("address and port of given key");
                    }
                    case "get-max" -> {
                        System.out.println("key value pair (max value)");
                    }
                    case "get-min" -> {
                        System.out.println("key value pair (min value)");
                    }
                    case "new-record" -> {
                        String[] newRecord = commands[++i].split(":");
                        key = Integer.parseInt(newRecord[0]);
                        value = Integer.parseInt(newRecord[1]);
                        System.out.println("OK");
                    }
                    case "terminate" -> {
                        System.out.println("remove node, inform the neighbours");
                        System.out.println("OK");
                    }
                }
            }

            // Terminate - close all the streams and the socket
            out.close();
            in.close();
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
