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
import java.util.*;

public class DatabaseNode extends Thread {
    private int key;
    private int value;
    private int port = 0;
    private boolean running = true;
    private String id = null;
    private final Set<String> neighbours = new HashSet<>();
    private final Map<String, Query> openQueries = new HashMap<>();

    public static void main(String[] args) throws IOException {
        new DatabaseNode(args).start();
    }
    // parameter storage


    public DatabaseNode(String[] args) throws IOException {
        super();


        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-tcpport" -> {
                    port = Integer.parseInt(args[++i]);
                }
                case "-record" -> {
                    String[] record = args[++i].split(":");
                    key = Integer.parseInt(record[0]);
                    value = Integer.parseInt(record[1]);
                }
                case "-connect" -> {
                    String nodeId = args[++i];
                    nodeId = nodeId.replaceAll("localhost", "127.0.0.1");
                    neighbours.add(nodeId);
                    String[] ipPort = nodeId.split(":");
                    try (Socket socket = new Socket(ipPort[0], Integer.parseInt(ipPort[1]))) {
                        id = socket.getLocalAddress() + ":" + port;
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        id = id.replaceAll("/", "");
                        out.println("add-me " + id);
                        out.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public void run() {
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket();
            System.setProperty("sun.net.useExclusiveBind", "false");
            serverSocket.setReuseAddress(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            serverSocket.bind(new InetSocketAddress(port));
            while (running) {
                Socket socket = serverSocket.accept();
                new RequestHandler(this, socket).start();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getPort() {
        return port;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public String getParentId() {
        return id;
    }

    public Set<String> getNeighbours() {
        return neighbours;
    }

    public Map<String, Query> getOpenQueries() {
        return openQueries;
    }

}