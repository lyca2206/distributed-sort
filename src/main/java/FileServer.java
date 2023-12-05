import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class FileServer {
    public void receiveFile(InputStream is) throws Exception {
        int filesize = 6022386;
        int bytesRead;
        int current = 0;
        byte[] mybytearray = new byte[filesize];

        File receivedFile = new File("def");

        try (FileOutputStream fos = new FileOutputStream(receivedFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            bytesRead = is.read(mybytearray, 0, mybytearray.length);
            current = bytesRead;

            do {
                bytesRead = is.read(mybytearray, current, (mybytearray.length - current));
                if (bytesRead >= 0) current += bytesRead;
            } while (bytesRead > -1);

            bos.write(mybytearray, 0, current);
            bos.flush();
        }
    }

    public static void main(String[] args) throws Exception {
        ServerSocket servsock = new ServerSocket(13267);
        while (true) {
            System.out.println("Waiting...");

            Socket sock = servsock.accept();
            System.out.println("Accepted connection : " + sock);
            InputStream is = sock.getInputStream();
            new FileServer().receiveFile(is);
            sock.close();
        }
    }
}
