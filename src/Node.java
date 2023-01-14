import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class Node {
    Socket netSocket;
    PrintWriter out;
    BufferedReader in;

    public Node(Socket netSocket, PrintWriter out, BufferedReader in) {
        this.netSocket = netSocket;
        this.out = out;
        this.in = in;
    }

    public Node(String address, String port) {
        try {
            System.out.println("Connecting with: " + address + " at port " + port);
            netSocket = new Socket(address, Integer.parseInt(port));
            out = new PrintWriter(netSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(netSocket.getInputStream()));
            System.out.println("Connected");
        } catch (UnknownHostException e) {
            System.err.println("Unknown host: " + address + ".");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("No connection with " + address + ".");
            System.exit(1);
        }
    }

    public void close() throws IOException {
        out.close();
        in.close();
        netSocket.close();
    }

    public PrintWriter out() {
        return out;
    }

    public BufferedReader in() {
        return in;
    }
}
