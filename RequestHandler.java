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
            String command;
            command = in.readLine();
            String[] commands = command.split(" ");
            for (int i = 0; i < commands.length; i++) {
                switch (commands[i]) {
                    case "set-value": {
                        String[] setValue = commands[++i].split(":");
                        String key = setValue[0];
                        String value = setValue[1];
                        System.out.println(parent.getParentId() + " Server is initiating operation set-value: set " + key + " to a new value " + value);
                        if (parent.getKey() == Integer.parseInt(key)) {
                            parent.setValue(Integer.parseInt(value));
                            System.out.println(parent.getParentId() + "-----------------VALUE SET----------------" + Integer.parseInt(value));
                            out.println("OK");
                        } else {
                            String ipPort = findKey(key);
                            if (ipPort.equals("ERROR")) {
                                System.out.println(parent.getParentId() + "-----------------VALUE NOT SET----------------" + "ERROR");
                                out.println("ERROR");
                            } else {
                                setValue(key, value, ipPort);
                                System.out.println(parent.getParentId() + "-----------------VALUE SET----------------" + ipPort);
                                out.println("OK");
                            }
                        }
                    }
                    break;
                    case "get-value": {
                        String key = commands[++i];
                        System.out.println(parent.getParentId() + " Server is initiating operation get-value: from key " + key);
                        if (parent.getKey() == Integer.parseInt(key)) {
                            System.out.println(parent.getParentId() + "-----------------GOT VALUE----------------" + key + ":" + parent.getValue());
                            out.println(key + ":" + parent.getValue());
                        } else {
                            String ipPort = findKey(key);
                            if (ipPort.equals("ERROR")) {
                                System.out.println(parent.getParentId() + "-----------------VALUE NOT FOUND----------------" + "ERROR");
                                out.println("ERROR");
                            } else {
                                String[] from = ipPort.split(":");
                                Socket s = new Socket(from[0], Integer.parseInt(from[1]));
                                PrintWriter outA = new PrintWriter(s.getOutputStream(), true);
                                BufferedReader inA = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                outA.println("give-value");
                                String read = inA.readLine();
                                System.out.println(parent.getParentId() + "-----------------GOT VALUE----------------" + key + ":" + key + ":" + read);
                                out.println(key + ":" + read);

                            }
                        }
                    }
                    break;
                    case "find-key": {
                        String key = commands[++i];
                        System.out.println(parent.getParentId() + " Server is initiating operation find-key: key searched " + key);
                        String ipPort = findKey(key);
                        System.out.println("-----------------FOUND KEY (or error)----------------" + ipPort);
                        out.println(ipPort);
                    }
                    break;
                    case "get-max": {
                        System.out.println(parent.getParentId() + " Server is initiating operation get-max");
                        String result = findInt(true);
                        System.out.println("-----------------FOUND MAX----------------" + result);
                        out.println("OK");
                    }
                    break;
                    case "get-min": {
                        System.out.println(parent.getParentId() + " Server is initiating operation get-min");
                        String result = findInt(false);
                        System.out.println("-----------------FOUND MIN----------------" + result);
                        out.println("OK");
                    }
                    break;
                    case "new-record": {
                        System.out.println(parent.getParentId() + " Server is adding new record");
                        String[] newRecord = commands[++i].split(":");
                        parent.setKey(Integer.parseInt(newRecord[0]));
                        parent.setValue(Integer.parseInt(newRecord[1]));
                        System.out.println("-----------------NEW RECORD----------------" + parent.getParentId());
                        out.println("OK");
                    }
                    break;
                    case "terminate": {
                        System.out.println(parent.getParentId() + " Server is terminating....");
                        terminate();
                        System.out.println(parent.getParentId() + "-----------------TERMINATE----------------");
                        out.println("OK");
                        in.close();
                        out.close();
                        socket.close();
                    }
                    break;
                    case "add-me": {
                        String ipPort = commands[++i];
                        parent.getNeighbours().add(ipPort);
                    }
                    break;
                    case "remove-me": {
                        String connect = commands[++i];
                        parent.getNeighbours().remove(connect);
                    }
                    break;
                    case "search-key": {
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
                        break;
                    }
                    case "no-key-found": {
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
                    break;
                    case "got-key": {
                        String uuid = commands[++i];
                        String result = commands[++i];
                        Query q = parent.getOpenQueries().get(uuid);
                        q.setResult(result);
                        q.close();
                        break;
                    }
                    case "give-value": {
                        out.println(parent.getValue());
                    }
                    break;
                    case "search-int": {
                        String uuid = commands[++i];
                        int key = Integer.parseInt(commands[++i]);
                        int result = Integer.parseInt(commands[++i]);
                        boolean max = Boolean.parseBoolean(commands[++i]);
                        String fromId = commands[++i];
                        if (parent.getOpenQueries().containsKey(uuid)) {
                            reverseInt(uuid, key, result, max, fromId);
                            continue;
                        }
                        if ((parent.getValue() > result && max) || (parent.getValue() < result && !max)) {
                            result = parent.getValue();
                            key = parent.getKey();
                        }
                        Query q = new Query(fromId);
                        for (String n : parent.getNeighbours()) {
                            if (n.equals(fromId)) continue;
                            q.addInvolved(n);
                        }
                        if (q.getInvolved().size() == 1) {
                            reverseInt(uuid, key, result, max, fromId);
                            continue;
                        }
                        parent.getOpenQueries().put(uuid, q);
                        searchInt(uuid, key, result, max, fromId);
                    }
                    break;
                    case "reverse-int": {
                        String uuid = commands[++i];
                        int key = Integer.parseInt(commands[++i]);
                        int result = Integer.parseInt(commands[++i]);
                        boolean max = Boolean.parseBoolean(commands[++i]);
                        String fromId = commands[++i];
                        Query q = parent.getOpenQueries().get(uuid);
                        if (q.getResultInt() == null) {
                            q.setResultInt(result);
                            q.setResultKey(key);
                        } else
                        if ((q.getResultInt() < result && max) || (q.getResultInt() > result && !max)) {
                            q.setResultInt(result);
                            q.setResultKey(key);
                        }
                        q.removeInvolved(fromId);
                        if (q.getInvolved().size() == 1) {
                            String toId = q.getInvolved().get(0);
                            if (!toId.equals(parent.getParentId())) {
                                reverseInt(uuid, q.getResultKey(), q.getResultInt(), max, toId);
                            } else {
                                q.close();
                            }
                        }
                    }
                    break;
                }
                in.close();
                out.close();
                socket.close();
            }
        } catch (IOException |
                 InterruptedException ex) {
            throw new RuntimeException(ex);
        }

    }

    private void reverseInt(String uuid, int key, int result, boolean max, String fromId) throws IOException {
        String[] ipPort = fromId.split(":");
        Socket s = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));
        PrintWriter out = new PrintWriter(s.getOutputStream(), true);
        System.out.println("....search-int-reverse-step...." + result + "-----" + uuid + " " + parent.getParentId() + " ----> " + fromId);
        out.println("reverse-int " + uuid + " " + key + " " + result + " " + max + " " + parent.getParentId());
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
        System.out.println("....search-key-reverse-step...." + uuid + " " + fromId + " ----> " + parent.getParentId());
        out.println("no-key-found " + uuid + " " + parent.getParentId());
    }

    private void terminate() throws IOException {
        for (String n : parent.getNeighbours()) {
            String[] params = n.split(":");
            Socket s = new Socket(params[0], Integer.parseInt(params[1]));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            out.println("remove-me " + parent.getParentId());
            out.close();
            s.close();
        }
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
        return q.getResult();
    }

    private void searchKey(String uuid, String key, String authorId, String fromId) throws IOException {
        for (String n : parent.getNeighbours()) {
            if (n.equals(fromId)) continue;
            String[] ipPort = n.split(":");
            Socket s = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            System.out.println("....search-key-step...." + uuid + " " + fromId + " ----> " + parent.getParentId());
            out.println("search-key " + uuid + " " + key + " " + authorId + " " + parent.getParentId());
        }
    }

    private String findInt(boolean max) throws IOException, InterruptedException {
        int result = parent.getValue();
        if (parent.getNeighbours().isEmpty()) {
            return parent.getKey() + ":" + result;
        }
        String uuid = UUID.randomUUID().toString();
        Query q = new Query(parent.getParentId());
        for (String n : parent.getNeighbours()) {
            q.addInvolved(n);
        }
        parent.getOpenQueries().put(uuid, q);
        searchInt(uuid, parent.getKey(), result, max, parent.getParentId());
        synchronized (parent.getOpenQueries().get(uuid)) {
            q.wait();
        }
        return q.getResultKey() + ":" + q.getResultInt();
    }

    private void searchInt(String uuid, int key, int result, boolean max, String fromId) throws IOException {
        for (String n : parent.getNeighbours()) {
            if (n.equals(fromId)) continue;
            String[] ipPort = n.split(":");
            Socket s = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            System.out.println("....search-int-step....." + result + "-----" + uuid + " " + fromId + "---->" + n);
            out.println("search-int " + uuid + " " + key + " " + result + " " + max + " " + parent.getParentId());
        }
    }
}
