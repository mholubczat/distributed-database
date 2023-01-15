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
import java.util.ArrayList;
import java.util.List;

public class DatabaseNode extends Thread {
    private int key;
    private int value;
    private ServerSocket serverSocket;
    private int port = 0;
    private boolean running = true;
    private List<Socket> parents = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        new DatabaseNode(args).start();
    }
    // parameter storage

    public DatabaseNode(String[] args) throws IOException {
        super();
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
                    parents.add(new Socket(connect[0], Integer.parseInt(connect[1])));
                }
            }
        }
    }

    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            while (running) {
                System.out.println("Server starts accepting connections on port " + port);
                Socket socket = serverSocket.accept();
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                System.out.println("Connected to server at " + socket.getInetAddress());

                String command = in.readLine();
                System.out.println("Server received command: " + command);
                assert command != null;
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
                            System.out.println("New key: " + key);
                            System.out.println("New value: " + value);
                            System.out.println("OK");
                        }
                        case "terminate" -> {
                            System.out.println("Server is terminating....");
                            terminate();
                            running = false;
                            out.println("OK");
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void terminate() throws IOException {
        for (Socket s : parents) {
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            out.println("Terminating");
            out.close();
            s.close();
        }
    }
}