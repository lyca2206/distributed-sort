import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.Socket;

public class FileClient {
    public void send(OutputStream os, String fileName) throws Exception {
        File myFile = new File(fileName);
        byte[] mybytearray = new byte[(int) myFile.length()];
        try (FileInputStream fis = new FileInputStream(myFile);
             BufferedInputStream bis = new BufferedInputStream(fis)) {
            bis.read(mybytearray, 0, mybytearray.length);
            System.out.println("Sending...");
            os.write(mybytearray, 0, mybytearray.length);
            os.flush();
        }
    }
}
