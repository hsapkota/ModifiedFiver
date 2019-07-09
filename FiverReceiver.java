import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FiverReceiver{
    static long totalTransferredBytes = 0L;
    static long totalChecksumBytes = 0L;
    long INTEGRITY_VERIFICATION_BLOCK_SIZE;
    boolean hasFinishedChecksum =false;
    static AtomicBoolean allTransfersCompleted = new AtomicBoolean(false);
    ChecksumConnector checksumConnector  = new ChecksumConnector();
    static LinkedBlockingQueue<Block> items = new LinkedBlockingQueue<>(100);
    static String baseDir = "/Users/earslan/receivedFiles/";
    LinkedList<DataStorage> data_store = new LinkedList<>();
    boolean debug = false;
    Long startTime = null;
    int[] ports = {2008, 2009, 2010, 2011, 2012};
    Long numberOfTransfers = 0L;
    Long numberOfCheckSum = 0L;
    Long max_cc = 4L;
    double ratio_check = 0.05;
    Long wait_for;


    public class MonitorThread extends Thread {
        long lastReceivedBytes = 0;
        long lastReadBytes = 0;
        String currentFile;
        int since_last_created = 0;
        Thread[] threads_transfer = new Thread[max_cc.intValue()];
        public void setCurrentFile(String currentFile) {
            this.currentFile = currentFile;
        }

        @Override
        public void run() {
            Thread checksumConnectThread = new Thread(checksumConnector, "checksumConnector");
            checksumConnectThread.start();
            wait_for = 5000L;
            String thread_name;

            for(int i=0;i<max_cc;i++) {
                FileReader fr = new FileReader(ports[numberOfTransfers.intValue()]);
                thread_name = "[R] fileTransferThread - " + numberOfTransfers;
                Thread fr_thread = new Thread(fr, thread_name);
                threads_transfer[i] = fr_thread;

                fr_thread.start();
            }


            ChecksumRunnable worker = new ChecksumRunnable();
            thread_name = "[R] checksumThread - " + numberOfCheckSum;
            Thread thread = new Thread(worker, thread_name);
            thread.start();


            try {
                int time = 0;
                while (!allTransfersCompleted.get()) {
                    double transferThrInMbps = 8 * (totalTransferredBytes - lastReceivedBytes) / (1000*1000);
                    double checksumThrInMbps = 8 * (totalChecksumBytes - lastReadBytes) / (1000*1000);
                    if (startTime != null){
                        System.out.println("Transfer Thr:" + transferThrInMbps + " Mb/s, Checksum thr:" + checksumThrInMbps + ", items:" + items.size() + " time:"
                                + (System.currentTimeMillis() - startTime) / 1000.0 + " s" + " tt: " + totalTransferredBytes/(1024.0*1024*1024) + " & ct:" + totalChecksumBytes/(1024.0*1024*1024));
                    }
                    lastReceivedBytes = totalTransferredBytes;
                    lastReadBytes = totalChecksumBytes;


                    if((since_last_created > 2) && (transferThrInMbps != 0) &&  (checksumThrInMbps!=0) && (transferThrInMbps-checksumThrInMbps)/(transferThrInMbps)>=ratio_check){
                        ChecksumRunnable n_ = new ChecksumRunnable();
                        thread_name = "[R] checksumThread - " + numberOfCheckSum;
                        Thread checksumThread0 = new Thread(n_, thread_name);
                        checksumThread0.start();
                        since_last_created=0;
                    }else if((transferThrInMbps==0  || checksumThrInMbps==0)||((transferThrInMbps != 0) &&(transferThrInMbps-checksumThrInMbps)/(transferThrInMbps)<ratio_check)){
                        since_last_created=0;
                    }else{
                        since_last_created++;
                    }
                    if(startTime!=null) {
                        time++;
                        data_store.add(new DataStorage(time, transferThrInMbps, checksumThrInMbps, numberOfCheckSum.intValue(), numberOfTransfers.intValue()));
                    }
                    Thread.sleep(1000);
                }
                print_dataStorage();
                for(int i=0;i<max_cc;i++) {
                    threads_transfer[i].interrupt();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("FEVER Receiver Total Time " + (System.currentTimeMillis() - startTime - wait_for)/1000.0 + " s");
            System.exit(0);
        }
    }


    public void print_dataStorage(){
        for(int i =0; i <data_store.size();i++){
            DataStorage sd = data_store.get(i);
            System.out.println(sd.time+", "+sd.transferThroughput+", "+sd.IOThroughput+" ,"+sd.noOfThreads+", "+sd.transferThreads);
        }
    }
    class DataStorage{
        public DataStorage(int time, double tT, double ioT, int cs, int t_th){
            this.time = time;
            this.transferThroughput = tT;
            this.IOThroughput = ioT;
            this.noOfThreads = cs;
            this.transferThreads = t_th;
        }
        int time;
        double transferThroughput;
        double IOThroughput;
        int noOfThreads;
        int transferThreads;
    }


    public static void main (String[] args) {
        if (args.length > 0) {
            baseDir = args[0];
        }
        FiverReceiver fs = new FiverReceiver();
    }

    public class FileReader implements Runnable{
        ServerSocket socket_receive = null;
        public FileReader(int port_id){
            try {

                synchronized (numberOfTransfers) {
                    socket_receive = new ServerSocket(port_id);
                    numberOfTransfers++;
                }
                socket_receive.setReceiveBufferSize(1024*1024);

            }catch(Exception e){
                e.printStackTrace();
            }
        }
        public void run(){
            System.out.println("MyThread - START " + Thread.currentThread().getName());
            try {
                Socket clientSock = socket_receive.accept();
                clientSock.setSoTimeout(10000);
                System.out.println("Connection established from  " + clientSock.getInetAddress());

                saveFile(clientSock);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        private void saveFile(Socket clientSock) throws IOException, InterruptedException {
            DataInputStream dataInputStream  = new DataInputStream(clientSock.getInputStream());
            INTEGRITY_VERIFICATION_BLOCK_SIZE = dataInputStream.readLong();
            wait_for = dataInputStream.readLong();

            if(startTime == null){
                startTime = System.currentTimeMillis();
            }

            allTransfersCompleted.set(false);
            totalTransferredBytes = 0L;
            totalChecksumBytes = 0L;

            byte[] buffer = new byte[128*1024];
            while(true) {
                long blockId = 0;
                String fileName = dataInputStream.readUTF();
                if(fileName.equalsIgnoreCase("done")){
                    break;
                }
                long offset = dataInputStream.readLong();
                long fileSize = dataInputStream.readLong();
                long fileId = dataInputStream.readLong();
                if (debug) {
                    System.out.println("File " + fileName + "\t" +
                            humanReadableByteCount(fileSize, false) + " bytes" +
                            " time:" + (System.currentTimeMillis() - startTime)/1000.0 + " s");
                }
                RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + fileName, "rw");

                if (offset > 0) {
                    randomAccessFile.getChannel().position(offset);
                }
                long remaining = fileSize;
                int read = 0;
                long transferStartTime = System.currentTimeMillis();
                Block itm = new Block((int) Math.min((int) INTEGRITY_VERIFICATION_BLOCK_SIZE, remaining));

                while (remaining > 0) {
                    read = dataInputStream.read(buffer, 0, (int) Math.min((long)Math.min(buffer.length, itm.length-itm.till_now), remaining));

                    if (read == -1)
                        break;
                    try {
                        itm.add_buffer(buffer, read);
                        totalTransferredBytes += read;
                        remaining -= read;


                    }catch(ArrayIndexOutOfBoundsException e1){
                        e1.printStackTrace();
                    }

                    if(itm.till_now >= (int)INTEGRITY_VERIFICATION_BLOCK_SIZE || remaining == 0){
                        itm.block_id = blockId++;
                        itm.fileId = fileId;

                        items.offer(itm, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                        itm = new Block((int) Math.min((int) INTEGRITY_VERIFICATION_BLOCK_SIZE, remaining));
                    }
                    randomAccessFile.write(buffer, 0, read);

                    buffer = new byte[128*1024];

                }
                randomAccessFile.close();
                if (read == -1) {
                    System.out.println("Read -1, closing the connection...");
                    return;
                }
                if (debug) {
                    System.out.println("Transfer End " + fileName + " size:" + fileSize +
                            " duration:" + (System.currentTimeMillis() - transferStartTime)/1000.0 +
                            " time:" + (System.currentTimeMillis() - startTime)/1000.0);
                }
            }
            System.out.println("Receiver is done!");
            dataInputStream.close();
            clientSock.close();
            allTransfersCompleted.set(true);
            hasFinishedChecksum = true;

        }
    }
    public FiverReceiver() {
        try {
            new MonitorThread().start();

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public class FiverFile {
        public FiverFile(File file, long offset, long length, long fileId) {
            this.file = file;
            this.offset = offset;
            this.length = length;
            this.fileId = fileId;
        }
        File file;
        Long offset;
        Long length;
        Long fileId;
        long blocks = 0;
    }

    class Buffer {
        byte[] small_buffer;
        int length;

        Buffer(byte[] buffer, int buffer_size){
            small_buffer = buffer;
            length = buffer_size;
        }
    }

    class Block {
        int till_now = 0;
        int length;
        long block_id;
        long fileId;
        List<Buffer> byte_array;


        Block(int length){
            this.length = length;
            byte_array = new ArrayList<Buffer>();
        }
        void add_buffer(byte[] bff, int buffer_size){
            byte_array.add(new Buffer(bff, buffer_size));
            till_now += buffer_size;

        }
        void remove_buffer(){
            this.byte_array = null;
            System.gc();
        }
    }

    public class ChecksumConnector implements Runnable{
        DataOutputStream  dataOutputStream = null;
        public void run(){
            System.out.println("MyThread - START "+Thread.currentThread().getName());
            try {
                ServerSocket socket  = new ServerSocket(20180);
                dataOutputStream  = new DataOutputStream(socket.accept().getOutputStream());
                System.out.println("Checksum Connection accepted");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public class ChecksumRunnable implements Runnable {
        MessageDigest md = null;
        DataOutputStream  dataOutputStream = null;

        long totalChecksumTime = 0;
        public ChecksumRunnable(){
            synchronized (numberOfCheckSum){
                numberOfCheckSum++;
            }
        }
        @Override
        public void run() {
            System.out.println("MyThread - START "+Thread.currentThread().getName());
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            md.reset();
            while(checksumConnector.dataOutputStream==null){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            dataOutputStream = checksumConnector.dataOutputStream;
            Long currentFileId = 0L;
            Long currentBlockId = 0L;
            while(!hasFinishedChecksum) {
                try {
                    Block item = items.poll(100, TimeUnit.MILLISECONDS);
                    if (item == null) {
                        continue;
                    }
                    currentFileId = item.fileId;
                    currentBlockId = item.block_id;

                    for (int i = 0; i < item.byte_array.size(); i++) {
                        Buffer chunk = item.byte_array.get(i);
                        md.update(chunk.small_buffer, 0, chunk.length);

                    }
                    byte[] digest = md.digest();
                    String hex = (new HexBinaryAdapter()).marshal(digest);

                    if(dataOutputStream != null) {
                        if(debug) {
                            System.out.println("Sending hex:" + hex + " size: " + item.length + " for fileId: " + currentFileId + " for blockId: " + currentBlockId);
                        }
                        synchronized (dataOutputStream) {
                            dataOutputStream.writeLong(currentFileId);
                            dataOutputStream.writeLong(currentBlockId);
                            dataOutputStream.writeUTF(hex);
                        }
                    }
                    item.remove_buffer();
                    md.reset();
                    totalChecksumBytes += item.length;

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

}