import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.io.*;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

public class FiverSender {
    long INTEGRITY_VERIFICATION_BLOCK_SIZE = 256 * 1024 * 1024;
    static LinkedBlockingQueue<Block> items = new LinkedBlockingQueue<>(100);
    static HashMap<String, Block> item_hashmap = new HashMap<>();
    List<FiverFile> files;
    Long startTime = null;
    boolean debug = false;
    static long fileId = 0;
    static String fileOrdering = "shuffle";
    static long totalTransferredBytes = 0;
    static long totalChecksumBytes = 0;
    static boolean allFileTransfersCompleted = false;
    boolean hasFinishedChecksum = false;
    Long numberOfTransfers = 0L;
    Long numberOfCheckSum = 0L;
    ChecksumConnector checksumConnector  = new ChecksumConnector();
    static String destIp;
    FiverSender fiver_sender = this;
    int[] ports = {2008, 2009, 2010, 2011, 2012, 2013, 2014};
    LinkedList<DataStorage> data_store = new LinkedList<>();
    double ratio_check = 0.05;
    Long max_cc = 4L;
    boolean file_empty = false;
    Long lock_until_process = 0L;
    Integer checksum_called=0;
    Long wait_for = 5000L;



    public class MonitorThread extends Thread {
        long lastTransferredBytes = 0;
        long lastChecksumBytes = 0;
        int since_last_created = 0;
        int since_last_created_transfer = 0;

        @Override
        public void run() {
            Thread checksumConnectThread = new Thread(checksumConnector, "checksumConnector");
            checksumConnectThread.start();

            FileSender fs = new FileSender(ports[numberOfTransfers.intValue()]);
            String thread_name = "[S] fileTransferThread - " + numberOfTransfers;
            Thread fs_thread = new Thread(fs, thread_name);
            fs_thread.start();

            /*
            FileSender fs1 = new FileSender(ports[numberOfTransfers.intValue()]);
            thread_name = "[S] fileTransferThread - " + numberOfTransfers;
            Thread fs_thread1 = new Thread(fs1, thread_name);
            fs_thread1.start();

            /*
            FileSender fs2 = new FileSender(ports[numberOfTransfers.intValue()]);
            thread_name = "[S] fileTransferThread - " + numberOfTransfers;
            Thread fs_thread2 = new Thread(fs2, thread_name);
            fs_thread2.start();

            //*/
            ChecksumThread worker = new ChecksumThread();
            thread_name = "[S] checksumThread - " + numberOfCheckSum;
            Thread thread = new Thread(worker, thread_name);
            thread.start();
            double previous_throughput = 1.0;
            double max_throughput = 1.0;
            try {
                int time = 0;
                while (!hasFinishedChecksum) {
                    double transferThrInMbps = 8 * (totalTransferredBytes-lastTransferredBytes)/(1000*1000);
                    double checksumThrInMbps = 8 * (totalChecksumBytes-lastChecksumBytes)/(1024*1024);
                    System.out.println("Network thr:" + transferThrInMbps + "Mb/s I/O thr:" + checksumThrInMbps + " Mb/s" +
                            " items:" + items.size() +" time:" + (System.currentTimeMillis() - startTime)/1000.0 + " s"+" tt: "+totalTransferredBytes/(1024.0*1024*1024)+" & ct:"+totalChecksumBytes/(1024.0*1024*1024));
                    lastTransferredBytes = totalTransferredBytes;
                    lastChecksumBytes = totalChecksumBytes;


                    if((since_last_created > 2) && (transferThrInMbps != 0) && (checksumThrInMbps!=0) && (transferThrInMbps-checksumThrInMbps)/(transferThrInMbps)>=ratio_check){
                        ChecksumThread n_ = new ChecksumThread();
                        thread_name = "[S] checksumThread - " + numberOfCheckSum;
                        Thread checksumThread0 = new Thread(n_,thread_name);
                        checksumThread0.start();
                        since_last_created=0;

                    }else if((transferThrInMbps == 0 || checksumThrInMbps==0)||((transferThrInMbps != 0) &&(transferThrInMbps-checksumThrInMbps)/(transferThrInMbps)<ratio_check)){
                        since_last_created=0;
                    }else{
                        since_last_created++;
                    }

                    if((numberOfTransfers<max_cc) && (since_last_created_transfer > 2) && (transferThrInMbps != 0) && (previous_throughput-transferThrInMbps)/(transferThrInMbps)>=ratio_check){
                        if(((max_throughput-transferThrInMbps)/max_throughput)<=0.5){
                            FileSender n_ = new FileSender(ports[numberOfTransfers.intValue()]);
                            thread_name = "[S] fileTransferThread - " + numberOfTransfers;
                            Thread transfer_thread = new Thread(n_, thread_name);
                            transfer_thread.start();
                            since_last_created_transfer=0;
                        }else{
                            since_last_created_transfer=0;
                        }

                    }else if((transferThrInMbps == 0)||((transferThrInMbps != 0) &&(previous_throughput-transferThrInMbps)/(transferThrInMbps)<ratio_check)){
                        since_last_created_transfer=0;
                    }else{
                        since_last_created_transfer++;
                    }
                    max_throughput = Math.max(max_throughput, transferThrInMbps);
                    previous_throughput = transferThrInMbps;
                    time++;
                    data_store.add(new DataStorage(time, transferThrInMbps, checksumThrInMbps, numberOfCheckSum.intValue(), numberOfTransfers.intValue()));

                    Thread.sleep(1000);
                }
                print_dataStorage();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("FEVER Sender Total Time " + (System.currentTimeMillis() - startTime - wait_for)/1000.0 + " s");
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

    public FiverSender() {
        try {
            new MonitorThread().start();

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void main(String[] args) {
        destIp = args[0];
        String path = args[1];
        if (args.length > 2) {
            fileOrdering = args[2];
        }
        int[] ports = {2008, 2009};
        FiverSender fc = new FiverSender();
        fc.collect_files(path);
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public void check_checksums(Block item){
        String key = item.fileId+"-"+item.block_id;
        if(item.hex_checked){
            item_hashmap.remove(key);
        }
        String hr = item.hex_received;
        String hc = item.hex_calculated;
        synchronized (checksum_called) {
            checksum_called-=1;
        }
        if(!hr.equalsIgnoreCase("") && !hc.equalsIgnoreCase("")){
            if(hr.equalsIgnoreCase(hc)){
                item_hashmap.remove(key);
                item.hex_checked = true;
                if(debug) {
                    System.out.println("[+] Checksum MATCHED of file " + item.file.getName() + " offset:" + item.offset +
                            " length:" + item.till_now + " time:" + (System.currentTimeMillis() - startTime) / 1000.0 + " fileId is " + item.fileId + " and BlockId is " + item.block_id);
                }
            }else{
                System.out.println("[-] Checksum is not matched of file "+item.file.getName()+ " fileId = "+item.fileId + " blockId = "+item.block_id + " receivedHex= " +item.hex_received+ " calculatedHex= "+item.hex_calculated);
                item_hashmap.remove(key);
                item.hex_checked = true;
                synchronized (files) {
                    files.add(new FiverFile(item.file, item.offset, item.till_now, fileId));
                    fileId++;
                }
            }
        }
    }
    private void collect_files(String path) {
        startTime = System.currentTimeMillis();
        synchronized (lock_until_process){
            File file = new File(path);
            files = new LinkedList<>();
            if (file.isDirectory()) {
                for (File f : file.listFiles()) {
                    files.add(new FiverFile(f, 0, f.length(), fileId));
                    fileId++;
                }
            } else {
                files.add(new FiverFile(file, 0, file.length(), fileId));
                fileId++;
            }
            System.out.println("Will transfer " + files.size() + " files");

            if (fileOrdering.compareTo("shuffle") == 0) {
                Collections.shuffle(files);
            } else if (fileOrdering.compareTo("sort") == 0) {
                Collections.sort(files, new Comparator<FiverFile>() {
                    public int compare(FiverFile f1, FiverFile f2) {
                        try {
                            int i1 = Integer.parseInt(f1.file.getName());
                            int i2 = Integer.parseInt(f2.file.getName());
                            return i1 - i2;
                        } catch (NumberFormatException e) {
                            throw new AssertionError(e);
                        }
                    }
                });
            } else {
                System.out.println("Undefined file ordering:" + fileOrdering);
                System.exit(-1);
            }
        }

    }

    public class FileSender implements Runnable{
        Socket socket_using = null;
        int port;
        public FileSender(int port_id){
            port = port_id;
            try {
                synchronized (numberOfTransfers) {
                    socket_using = new Socket(destIp, port);
                    numberOfTransfers++;
                }
                socket_using.setSoTimeout(10000);
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        public void run(){
            System.out.println("MyThread - START " + Thread.currentThread().getName() + " using port "+port);
            try {
                sendFile(socket_using);
            }catch(IOException e){
                e.printStackTrace();
            }
        }
        public void sendFile(Socket s) throws IOException {
            synchronized (lock_until_process){
                lock_until_process = 1L;
            }
            DataOutputStream dos = new DataOutputStream(s.getOutputStream());
            dos.writeLong(INTEGRITY_VERIFICATION_BLOCK_SIZE);
            dos.writeLong(wait_for);
            if(startTime == null){
                startTime = System.currentTimeMillis();
            }

            byte[] buffer = new byte[128*1024];
            int n;
            while (!hasFinishedChecksum) {
                FiverFile currentFile = null;
                synchronized (files) {
                    if (!files.isEmpty()) {
                        currentFile = files.remove(0);
                    }else{
                        file_empty = true;
                    }
                }
                Long blockId = 0L;
                if (currentFile == null) {
                    try {
                        Thread.sleep(100);
                        file_empty = true;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

                //checksumFiles.offer(currentFile);
                //send file metadata
                dos.writeUTF(currentFile.file.getName());
                dos.writeLong(currentFile.offset);
                dos.writeLong(currentFile.length);
                dos.writeLong(currentFile.fileId);
                if (debug) {
                    System.out.println("Transfer START file " + currentFile.file.getName() + "offset" + currentFile.offset +
                            " size:" + humanReadableByteCount(currentFile.file.length(), false) + " time:" +
                            (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
                }
                long fileTransferStartTime = System.currentTimeMillis();

                FileInputStream fis = new FileInputStream(currentFile.file);
                if (currentFile.offset > 0) {
                    fis.getChannel().position(currentFile.offset);
                }
                Long remaining = currentFile.length;
                Long offset = 0L;
                try {
                    Block itm = new Block((int) Math.min((int) INTEGRITY_VERIFICATION_BLOCK_SIZE, remaining));
                    file_empty = false;

                    while ((n = fis.read(buffer, 0, (int) Math.min((long)Math.min(buffer.length, itm.length-itm.till_now), remaining))) > 0) {
                        hasFinishedChecksum = false;
                        remaining -= n;
                        totalTransferredBytes += n;

                        itm.add_buffer(buffer, n);


                        if(itm.till_now >= (int)INTEGRITY_VERIFICATION_BLOCK_SIZE || remaining == 0){
                            itm.block_id = blockId;
                            itm.file = currentFile.file;
                            itm.offset = offset;
                            blockId++;

                            itm.fileId = currentFile.fileId;

                            items.offer(itm, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                            item_hashmap.put(""+itm.fileId+"-"+itm.block_id, itm);
                            synchronized (checksum_called) {
                                checksum_called+=2;
                            }

                            itm = new Block((int) Math.min((int) INTEGRITY_VERIFICATION_BLOCK_SIZE, remaining));
                            offset = currentFile.length - remaining;

                        }
                        dos.write(buffer, 0, n);

                        buffer = new byte[128*1024];
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (debug) {
                    System.out.println("Transfer END file " + currentFile.file.getName() + "\t duration:" +
                            (System.currentTimeMillis() - fileTransferStartTime) / 1000.0 + " time:" +
                            (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
                }
                fis.close();
            }
            dos.writeUTF("done");
            allFileTransfersCompleted = true;
            System.out.println("Sender is done!");
        }
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

    class Buffer{
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
        String hex_received = "";
        String hex_calculated = "";
        boolean hex_checked = false;
        File file;
        Long offset;
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
        DataInputStream  dataInputStream = null;

        public void run(){
            System.out.println("MyThread - START " + Thread.currentThread().getName());
            while (true) {
                try {
                    Socket s = new Socket(destIp, 20180);
                    dataInputStream = new DataInputStream(s.getInputStream());
                    break;
                } catch (IOException e) {
                    System.out.println("Trying to connect to checksum thread");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }
            Long wait_time = wait_for;
            while((!file_empty) || wait_time>0L || checksum_called>0) {

                try {
                    Long currentFileId;
                    Long currentBlockId;
                    String destinationHex;
                    if(dataInputStream.available()>0) {
                        synchronized (dataInputStream) {
                            currentFileId = dataInputStream.readLong();
                            currentBlockId = dataInputStream.readLong();
                            destinationHex = dataInputStream.readUTF();
                        }
                        String key = "" + currentFileId + "-" + currentBlockId;

                        Block itm = item_hashmap.get(key);

                        if (itm != null) {
                            itm.hex_received = destinationHex;
                            fiver_sender.check_checksums(itm);
                        }
                        wait_time = wait_for;
                    }else{
                        try{
                            Thread.sleep(100);
                            wait_time -= 100L;
                        }catch(Exception e2){
                            e2.printStackTrace();
                        }
                        if (wait_time <= 0L && (!items.isEmpty() || !files.isEmpty())) {
                            if (checksum_called > 0) {
                                for (Map.Entry<String, Block> entry : item_hashmap.entrySet()) {
                                    Block itm = entry.getValue();
                                    itm.hex_received = "a";
                                    fiver_sender.check_checksums(itm);
                                }
                                wait_time = wait_for;
                            }
                        }
                        if(!items.isEmpty() || !files.isEmpty()){
                            wait_time = wait_for;
                        }
                    }

                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            hasFinishedChecksum = true;

        }
    }


    public class ChecksumThread implements Runnable {
        MessageDigest md = null;
        DataInputStream  dataInputStream = null;
        public ChecksumThread(){
            synchronized (numberOfCheckSum){
                numberOfCheckSum++;
            }
        }
        public void reset () {
            md.reset();
        }
        @Override
        public void run() {
            System.out.println("MyThread - START " + Thread.currentThread().getName());


            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            md.reset();

            while(checksumConnector.dataInputStream == null){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            dataInputStream = checksumConnector.dataInputStream;
            int wait_for = 10;
            while(!hasFinishedChecksum) {
                try {
                    Block item = items.poll(100, TimeUnit.MILLISECONDS);
                    if (item == null) {
                        if(files.isEmpty()){
                            wait_for--;
                        }
                        else{
                            wait_for = 10;
                        }
                        continue;
                    }
                    int usedBytes = item.length;

                    for (int i = 0; i < item.byte_array.size(); i++) {
                        Buffer chunk = item.byte_array.get(i);
                        md.update(chunk.small_buffer, 0, chunk.length);
                    }
                    byte[] digest = md.digest();
                    String hex = (new HexBinaryAdapter()).marshal(digest);
                    item.hex_calculated = hex;
                    totalChecksumBytes += usedBytes;
                    item.remove_buffer();
                    fiver_sender.check_checksums(item);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}