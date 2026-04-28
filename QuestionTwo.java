import java.util.Random;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.BlockingQueue;

public class QuestionTwo {

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<Integer> clientToServer = new SynchronousQueue<>();
        BlockingQueue<Integer> serverToClient = new SynchronousQueue<>();
        BlockingQueue<String> dataChannel = new SynchronousQueue<>();

        Client client = new Client(clientToServer, serverToClient, dataChannel);
        Server server = new Server(clientToServer, serverToClient, dataChannel);

        new Thread(server).start();
        new Thread(client).start();
    }

    // --- Client Logic ---
    static class Client implements Runnable {
        private final BlockingQueue<Integer> tx; // transmit
        private final BlockingQueue<Integer> rx; // receive
        private final BlockingQueue<String> data;
        private final Random rand = new Random();

        Client(BlockingQueue<Integer> tx, BlockingQueue<Integer> rx, BlockingQueue<String> data) {
            this.tx = tx; this.rx = rx; this.data = data;
        }

        @Override
        public void run() {
            try {
                int clientSyn = rand.nextInt(100);
                System.out.println("[Client] Sent SYN: " + clientSyn);
                tx.put(clientSyn); 

                int serverAck = rx.take(); 
                int serverSyn = rx.take(); 
                System.out.println("[Client] Received ACK: " + serverAck + " and SYN: " + serverSyn);

                if (serverAck == clientSyn + 1) {
                    int finalAck = serverSyn + 1;
                    System.out.println("[Client] Sending final ACK: " + finalAck);
                    tx.put(finalAck); 
                    
                    String pkg = data.take();
                    System.out.println("[Client] Successfully received: " + pkg);
                } else {
                    System.out.println("[Client] Failed");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // --- Server Logic ---
    static class Server implements Runnable {
        private final BlockingQueue<Integer> tx; 
        private final BlockingQueue<Integer> rx; 
        private final BlockingQueue<String> data;
        private final Random rand = new Random();

        Server(BlockingQueue<Integer> rx, BlockingQueue<Integer> tx, BlockingQueue<String> data) {
            this.rx = rx; this.tx = tx; this.data = data;
        }

        @Override
        public void run() {
            try {
                int clientSyn = rx.take(); 
                System.out.println("[Server] Received SYN: " + clientSyn);

                int serverAck = clientSyn + 1;
                int serverSyn = rand.nextInt(100) + 100;

                System.out.println("[Server] Sending ACK: " + serverAck + " and SYN: " + serverSyn);
                tx.put(serverAck); 
                tx.put(serverSyn); 

                int finalAck = rx.take(); 
                System.out.println("[Server] Received final ACK: " + finalAck);

                if (finalAck == serverSyn + 1) {
                    System.out.println("[Server] Complete. Sending Data...");
                    data.put("Secret Server Package");
                } else {
                    System.out.println("[Server] Fail");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}