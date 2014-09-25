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

    private static void startServer(int port) throws IOException {
        server = new NettyServer(new SpecificResponder(Mail.class, new MailImpl()), new InetSocketAddress(port));
        // the server implements the Mail protocol (MailImpl)
    }

    public static void main(String[] args) throws Exception {
	if(args.length < 2)
	    throw new Exception("Lack of arguments. Expected at least two args: <mode> <port>, where <mode> = server | client ");
	String mode = args[0];
	int port = Integer.parseInt(args[1]);
        if("server".equals(mode))
	    asServer(port, args);
	else if ("client".equals(mode))
	    asClient(port, args);
	else throw new Exception("Bad mode: " + mode + ". Expected: server | client .");

	System.exit(0);
    }

    private static void asServer(int port, String[] args) throws IOException {
	System.out.println("Starting server");

        startServer(port);

	System.out.println("*DONE*");

	System.out.flush();
	System.in.read();
    }

    private static void asClient(int port, String[] args) throws IOException {
	NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(port));
        // // client code - attach to the server and send a message
        Mail proxy = (Mail) SpecificRequestor.getClient(Mail.class, client);
        System.out.println("Client built, got proxy");

         // fill in the Message record and send it
         Message message = new Message();
         message.setTo(new Utf8("TTTOOO"));
         message.setFrom(new Utf8("FFFROMMM"));
         message.setBody(new Utf8("MSGMSGMSG"));
         System.err.println("Calling proxy.send with message:  " + message.toString());
         System.err.println("Result: " + proxy.send(message));
	 System.out.println("*DONE*");
       // cleanup
         client.close();
    }
}
