package pt.ulisboa.tecnico.veilgraph.stream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * Stream provider for data coming from a socket
 *
 * @author Renato Rosa
 */
public class SocketStreamProvider extends StreamProvider<String> {

    private final String host;
    private final int port;

    public SocketStreamProvider(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void run() {
        try (final Socket s = new Socket(this.host, this.port);
            final BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                super.queue.put(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
