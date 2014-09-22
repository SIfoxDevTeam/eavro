package email;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.SocketServer;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;

import example.proto.Mail;
import example.proto.Message;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Main {
    public static class MailImpl implements Mail {
        // in this simple example just return details of the message
        public Utf8 send(Message message) {
            System.out.println("Sending message");
            return new Utf8("Sending message to " + message.getTo().toString()
                    + " from " + message.getFrom().toString()
                    + " with body " + message.getBody().toString());
        }
    }

    private static Server server;

    private static void startServer() throws IOException {
        server = new NettyServer(new SpecificResponder(Mail.class, new MailImpl()), new InetSocketAddress(65111));
        // the server implements the Mail protocol (MailImpl)
    }

    public static void main(String[] args) throws IOException {

        System.out.println("Starting server");

        startServer();
        System.out.println("Server started");

	System.out.flush();
	System.in.read();
	System.exit(1);
    }
}
