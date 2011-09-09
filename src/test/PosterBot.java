package test;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by IntelliJ IDEA.
 * User: Jeff Thomas
 * Date: 4/7/11
 * Time: 4:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class PosterBot {
    private String fileNameIn = null;
    private String fileNameOut = null;
    private String poleType = null;
    private Random random = null;

    private File fileIn;
    private File fileOut;

    public static boolean run = false;
    public static boolean exit = false;

    public PosterBot(final String serverUrl, final File fileIn, final File fileOut, String poleType, Random random) throws FileNotFoundException{
        this.poleType = poleType;
        this.random = random;

        final FileInputStream fisIn = new FileInputStream(fileIn);
        final FileInputStream fisOut = new FileInputStream(fileOut);

        fileNameIn = fileIn.getName() + ": ";
        fileNameOut = fileOut.getName() + ": ";

        final Executor messageExecutors = Executors.newCachedThreadPool();

/*
        fileIn = new File(fileNameIn);
        if (!fileIn.canRead()){
            throw(new FileNotFoundException("Could not find:" + fileNameIn));
        }
        fileOut = new File(fileNameOut);
        if (!fileOut.canRead()){
            throw(new FileNotFoundException("Could not find:" + fileNameOut));
        }
*/
        Executor executor = Executors.newCachedThreadPool();
        executor.execute(new Runnable() {
            public void run() {
                readPollServer(fisIn, serverUrl);
            }
        });

        executor.execute(new Runnable() {
            public void run() {
                writePollServer(fisOut, serverUrl);
            }
        });
    }

    private void readPollServer(InputStream isIn, String serverUrl) {
        try {
            BufferedReader brIn = new BufferedReader(new InputStreamReader(isIn));

            ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally

            long seqid = -1;

            //URL url = new URL("http://"+serverUrl+":8080/pollvault/?channel=testBooks&type=LONG_POLL");
            while (!exit) {
                try {
                    Thread.sleep(1);
                    if (run) {
                        URL url = new URL("http://"+serverUrl+":8085/longpoll?topic=testBooks&seqid=" + seqid);
                        //System.out.println(fileNameIn + ": " + "Polling with seqid: " + seqid);
                        BufferedReader in = new BufferedReader(new InputStreamReader((url).openStream()));
                        StringBuilder sb = new StringBuilder();
                        try {
                            String str;
                            while ((str = in.readLine()) != null) {
                                sb.append(str);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                            continue;
                        }
                        in.close();

                        if (sb.indexOf("{\"result\":\"OK\",\"message\":[]}") > -1){
                            continue;
                        }

                        //System.out.println(fileNameIn + ": " + sb.toString());

                        Map<String,Object> payload = mapper.readValue(sb.toString(), HashMap.class);

                        List<String> messages = (List<String>)payload.get("message");//mapper.readValue((String)payload.get("payload"), ArrayList.class);

                        seqid = Long.parseLong(""+payload.get("seqid"));

                        for (String message: messages){
                            System.out.println(fileNameIn + ": <-- " + message);
                            if (!message.startsWith(fileNameIn)){
                                //System.out.println(fileNameIn + ": rejected");
                                continue;
                            }
                            String strLine = "";
                            while (strLine != null && "".equals(strLine.trim())){
                                strLine = brIn.readLine();
                            }

                            if (!message.equals(fileNameIn + strLine)){
                                System.out.println("!!!!!" + message + " != " + fileNameIn+strLine);
                                exit = true;
                            } else {
                                System.out.println(fileNameIn + ": Accepted : " + message);
                            }
                        }
                    }

                } catch (InterruptedException e) {
                    exit = true;
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } catch (IOException e) {
                    exit = true;
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        exit = true;
    }

    private void writePollServer(InputStream isOut, String serverUrl) {
        try {
            //FileInputStream fstreamOut = new FileInputStream(fileNameIn);
            // Get the object of DataInputStream
            //DataInputStream dsOut = new DataInputStream(fstreamOut);
            //BufferedReader brOut = new BufferedReader(new InputStreamReader(dsOut));

            BufferedReader brOut = new BufferedReader(new InputStreamReader(isOut));

            Thread.sleep(5000);
            while (!exit) {
                try {
                    Thread.sleep(10);
                    if (run) {
                        String strLine = "";
                        while (strLine != null && "".equals(strLine.trim())){
                            strLine = brOut.readLine();
                        }

                        if (strLine == null){
                            exit = true;
                            break;
                        }

                        System.out.println("--> " + fileNameOut + strLine);
                        post(serverUrl, fileNameOut + strLine);
                    }

                } catch (InterruptedException e) {
                    exit = true;
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } catch (IOException e) {
                    exit = true;
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        exit = true;
    }

    private void post(String url, String line){
        try {
            // Construct data
            String data = URLEncoder.encode("topic", "UTF-8") + "=" + URLEncoder.encode("testBooks", "UTF-8");
            data += "&" + URLEncoder.encode("message", "UTF-8") + "=" + URLEncoder.encode(line, "UTF-8");

            // Create a socket to the host
            int port = 8085;
            InetAddress addr = InetAddress.getByName(url);
            Socket socket = new Socket(addr, port);

            // Send header
            String path = "/post";
            BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF8"));
            wr.write("POST "+path+" HTTP/1.0\r\n");
            wr.write("Content-Length: "+data.length()+"\r\n");
            wr.write("Content-Type: application/x-www-form-urlencoded\r\n");
            wr.write("\r\n");

            // Send data
            wr.write(data);
            wr.flush();

            // Get response
            BufferedReader rd = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String input = rd.readLine();
            if (!"HTTP/1.1 200 OK".equals(input)){
                System.out.println("Post return not ok.");
                System.out.println(input);
                while ((input = rd.readLine()) != null) {
                    System.out.println(input);
                }
                exit = true;
            }

            wr.close();
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
            exit = true;
        }
    }

    public static void main(String args[]){
        try {

            File roughFile = new File("/opt/Roughing It.txt");
            File abroadFile = new File("/opt/The Innocents Abroad.txt");

            PosterBot pbRough = new PosterBot("24.7.76.113",roughFile,roughFile,"LONG_POLLING",null);
            PosterBot pbAbroad = new PosterBot("24.7.76.113",abroadFile,abroadFile,"LONG_POLLING",null);

            PosterBot.run = true;

            while(!PosterBot.exit);

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}

