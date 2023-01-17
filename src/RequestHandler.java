import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.UUID;

public class RequestHandler extends Thread {
    private DatabaseNode parent;
    private Socket socket;

    public RequestHandler(DatabaseNode parent, Socket socket) {
        super();
        this.socket = socket;
        this.parent = parent;
    }

    public void run() {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            if (parent.getParentId() == null) {
                parent.setId((socket.getLocalAddress() + ":" + parent.getPort()).replaceAll("/", ""));
            }
            System.out.println("Connected to server " + parent.getParentId());
            String command;
            command = in.readLine();
            String[] commands = command.split(" ");
            for (int i = 0; i < commands.length; i++) {
                switch (commands[i]) {
                    case "set-value" -> {
                        String[] setValue = commands[++i].split(":");
                        String key = setValue[0];
                        String value = setValue[1];
                        System.out.println("Server is setting " + key + " to a new value " + value);
                        if (parent.getKey() == Integer.parseInt(key)) {
                            parent.setValue(Integer.parseInt(value));
                            out.println("OK");
                        } else {
                            String ipPort = findKey(key);
                            if (ipPort.equals("ERROR")) {
                                out.println("ERROR");
                            } else {
                                setValue(key, value, ipPort);
                                out.println("OK");
                            }
                        }
                    }
                    case "get-value" -> {
                        String key = commands[++i];
                        if (parent.getKey() == Integer.parseInt(key)) {
                            out.println(key + ":" + parent.getValue());
                        } else {
                            System.out.println("Server is searching " + key + " value");
                            String ipPort = findKey(key);
                            if (ipPort.equals("ERROR")) {
                                out.println("ERROR");
                            } else {
                                String[] from = ipPort.split(":");
                                Socket s = new Socket(from[0], Integer.parseInt(from[1]));
                                PrintWriter outA = new PrintWriter(s.getOutputStream(), true);
                                BufferedReader inA = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                outA.println("give-value");
                                out.println(key + ":" + inA.readLine());
                            }
                        }
                    }
                    case "find-key" -> {
                        String key = commands[++i];
                        System.out.println("Server is searching " + key);
                        String ipPort = findKey(key);
                        out.println(ipPort);
                    }
                    case "get-max" -> {
                        int result = findInt(true);
                    }
                    case "get-min" -> {
                        int result = findInt(false);
                    }
                    case "new-record" -> {
                        String[] newRecord = commands[++i].split(":");
                        parent.setKey(Integer.parseInt(newRecord[0]));
                        parent.setValue(Integer.parseInt(newRecord[1]));
                        System.out.println("New key: " + parent.getKey());
                        System.out.println("New value: " + parent.getValue());
                        out.println("OK");
                    }
                    case "terminate" -> {
                        System.out.println("Server is terminating....");
                        terminate();
                        out.println("OK");
                        in.close();
                        out.close();
                        socket.close();
                    }
                    case "add-me" -> {
                        String ipPort = commands[++i];
                        String[] params = ipPort.split(":");
                        System.out.println("Adding newly connected node " + params[0] + " " + params[1]);
                        parent.getNeighbours().add(ipPort);
                    }
                    case "remove-me" -> {
                        String connect = commands[++i];
                        parent.getNeighbours().remove(connect);
                        String[] params = connect.split(":");
                        for (String n : parent.getNeighbours()) {
                            System.out.println(n);
                        }
                        System.out.println("Removed node " + params[0] + " " + params[1]);
                        for (String n : parent.getNeighbours()) {
                            System.out.println(n);
                        }
                    }
                    case "search-key" -> {
                        System.out.println("-------Received search request");
                        String uuid = commands[++i];
                        String key = commands[++i];
                        String authorId = commands[++i];
                        String fromId = commands[++i];
                        if (parent.getOpenQueries().containsKey(uuid)) {
                            noKeyFound(uuid, fromId);
                            continue;
                        }
                        if (parent.getKey() == Integer.parseInt(key)) {
                            String[] ipPort = authorId.split(":");
                            Socket s = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));
                            PrintWriter outA = new PrintWriter(s.getOutputStream(), true);
                            outA.println("got-key " + uuid + " " + parent.getParentId());
                            continue;
                        }
                        Query q = new Query(fromId);
                        for (String n : parent.getNeighbours()) {
                            if (n.equals(fromId)) continue;
                            q.addInvolved(n);
                        }
                        if (q.getInvolved().size() == 1) {
                            noKeyFound(uuid, fromId);
                            continue;
                        }
                        parent.getOpenQueries().put(uuid, q);
                        searchKey(uuid, key, authorId, fromId);
                    }
                    case "no-key-found" -> {
                        String uuid = commands[++i];
                        String fromId = commands[++i];
                        Query q = parent.getOpenQueries().get(uuid);
                        q.removeInvolved(fromId);
                        if (q.getInvolved().size() == 1) {
                            String toId = q.getInvolved().get(0);
                            if (!toId.equals(parent.getParentId())) {
                                noKeyFound(uuid, q.getInvolved().get(0));
                            } else {
                                q.setResult("ERROR");
                                q.close();
                            }
                        }
                    }
                    case "got-key" -> {
                        System.out.println("--------------------------__GOT FN KEY----------------------------");
                        String uuid = commands[++i];
                        String result = commands[++i];
                        Query q = parent.getOpenQueries().get(uuid);
                        q.setResult(result);
                        q.close();
                    }
                    case "give-value" -> {
                        out.println(parent.getValue());
                    }
                    case "search-int" -> {
                        System.out.println("-------Received search INT request");
                        String uuid = commands[++i];
                        int result = Integer.parseInt(commands[++i]);
                        boolean max = Boolean.parseBoolean(commands[++i]);
                        String fromId = commands[++i];
                        if (parent.getOpenQueries().containsKey(uuid)) {
                            returnInt(uuid, result, max, fromId);
                            continue;
                        }
                        if ((parent.getValue() > result && max) || (parent.getValue() < result && max)) {
                            result = parent.getValue();
                        }
                        Query q = new Query(fromId);
                        for (String n : parent.getNeighbours()) {
                            if (n.equals(fromId)) continue;
                            q.addInvolved(n);
                        }
                        if (q.getInvolved().size() == 1) {
                            returnInt(uuid, result, max, fromId);
                            continue;
                        }
                        parent.getOpenQueries().put(uuid, q);
                        searchInt(uuid, result, max, fromId);
                    }
                    case "return-int" -> {
                        String uuid = commands[++i];
                        int result = Integer.parseInt(commands[++i]);
                        boolean max = Boolean.parseBoolean(commands[++i]);
                        String fromId = commands[++i];
                        Query q = parent.getOpenQueries().get(uuid);
                        if (q.getResultInt() == null) q.setResultInt(result);
                        if ((q.getResultInt() < result && max) || (q.getResultInt() > result && max)) {
                            q.setResultInt(result);
                        }
                        q.removeInvolved(fromId);
                        if (q.getInvolved().size() == 1) {
                            String toId = q.getInvolved().get(0);
                            if (!toId.equals(parent.getParentId())) {
                                returnInt(uuid, q.getResultInt(), max, toId);
                            } else {
                                q.close();
                            }
                        }
                    }
                }
                System.out.println("Socket will be closed now!");
                in.close();
                out.close();
                socket.close();
            }
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void returnInt(String uuid, int result, boolean max, String fromId) throws IOException {
        String[] ipPort = fromId.split(":");
        Socket s = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));
        PrintWriter out = new PrintWriter(s.getOutputStream(), true);
        out.println("return-int " + uuid + " " + result + " " + max + " " + parent.getParentId()); // czemu to ID?
    }

    private void setValue(String key, String value, String fromId) throws IOException {
        String[] ipPort = fromId.split(":");
        Socket s = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));
        PrintWriter out = new PrintWriter(s.getOutputStream(), true);
        out.println("new-record " + key + ":" + value);
    }

    private void noKeyFound(String uuid, String fromId) throws IOException {
        String[] ipPort = fromId.split(":");
        Socket s = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));
        PrintWriter out = new PrintWriter(s.getOutputStream(), true);
        out.println("no-key-found " + uuid + " " + parent.getParentId());
    }

    private void terminate() throws IOException {
        for (String n : parent.getNeighbours()) {
            System.out.println("Terminate " + n);
            String[] params = n.split(":");
            Socket s = new Socket(params[0], Integer.parseInt(params[1]));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            out.println("remove-me " + parent.getParentId());
            out.close();
            s.close();
        }
        System.out.println("setting running");
        parent.setRunning(false);
        String[] params = parent.getParentId().split(":");
        Socket s = new Socket(params[0], Integer.parseInt(params[1]));
        PrintWriter out = new PrintWriter(s.getOutputStream(), true);
        out.println("die");
    }

    private String findKey(String key) throws IOException, InterruptedException {
        if (parent.getKey() == Integer.parseInt(key)) {
            return parent.getParentId();
        }
        if (parent.getNeighbours().isEmpty()) {
            return "ERROR";
        }
        String uuid = UUID.randomUUID().toString();
        Query q = new Query(parent.getParentId());
        for (String n : parent.getNeighbours()) {
            q.addInvolved(n);
        }
        searchKey(uuid, key, parent.getParentId(), parent.getParentId());
        parent.getOpenQueries().put(uuid, q);
        synchronized (parent.getOpenQueries().get(uuid)) {
            q.wait();
        }
        System.out.println("THANK YOU HERO ------------------------------" + q.getResult());
        return q.getResult();
    }

    private void searchKey(String uuid, String key, String authorId, String fromId) throws IOException {
        for (String n : parent.getNeighbours()) {
            if (n.equals(fromId)) continue;
            String[] ipPort = n.split(":");
            Socket s = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            System.out.println("---------------search-key----------------" + uuid + " " + key + " " + authorId);
            out.println("search-key " + uuid + " " + key + " " + authorId + " " + parent.getParentId());
        }
    }

    private int findInt(boolean max) throws IOException, InterruptedException {
        int result = parent.getValue();
        if (parent.getNeighbours().isEmpty()) {
            return result;
        }
        String uuid = UUID.randomUUID().toString();
        Query q = new Query(parent.getParentId());
        for (String n : parent.getNeighbours()) {
            q.addInvolved(n);
        }
        searchInt(uuid, result, max, parent.getParentId());
        parent.getOpenQueries().put(uuid, q);
        synchronized (parent.getOpenQueries().get(uuid)) {
            q.wait();
        }
        System.out.println("THANK YOU HERO ------------------------------" + q.getResult());
        return q.getResultInt();
    }

    private void searchInt(String uuid, int result, boolean max, String fromId) throws IOException {
        for (String n : parent.getNeighbours()) {
            if (n.equals(fromId)) continue;
            String[] ipPort = n.split(":");
            Socket s = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            System.out.println("---------------search-int----------------" + uuid + " " + fromId);
            out.println("search-int " + uuid + " " + result + " " + max + " " + parent.getParentId());
        }
    }
}
