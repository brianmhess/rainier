package hessian.rainier;

import com.datastax.driver.core.ConsistencyLevel;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class RainierParams {
    public String host = null;
    public int port = 9042;
    public String username = null;
    public String password = null;
    public String truststorePath = null;
    public String truststorePwd = null;
    public String keystorePath = null;
    public String keystorePwd = null;
    public String inputFname = null;
    public int numThreads = 1;
    public ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_ONE;
    private String argfile = null;
    public Map<String,String> argmap = new HashMap<>();
    private String argstring = null;
    public Map<String,String> argfilemap = new HashMap<>();
    public long numIterations = 1000;
    public int minRepeat = 1;
    public int maxRepeat = 1;
    public int rate = 50000;

    public static String usage() {
        StringBuilder usage = new StringBuilder();
        usage.append("Usage: rainier -host <hostname> -f <input file>\n");
        usage.append("OPTIONS:\n");
        usage.append("  -host <hostname>               Contact point for DSE [required]\n");
        usage.append("  -f <input file>                File of queries to run\n");
        usage.append("  -configFile <filename>         File with configuration options [none]\n");
        usage.append("  -port <portNumber>             CQL Port Number [9042]\n");
        usage.append("  -user <username>               Cassandra username [none]\n");
        usage.append("  -pw <password>                 Password for user [none]\n");
        usage.append("  -ssl-truststore-path <path>    Path to SSL truststore [none]\n");
        usage.append("  -ssl-truststore-pw <pwd>       Password for SSL truststore [none]\n");
        usage.append("  -ssl-keystore-path <path>      Path to SSL keystore [none]\n");
        usage.append("  -ssl-keystore-pw <pwd>         Password for SSL keystore [none]\n");
        usage.append("  -numThreads <numThreads>       How many parallel queries to run [1]\n");
        usage.append("  -consistencyLevel <CL>         Consistency Level [LOCAL_ONE]\n");
        usage.append("  -arg <key:val,...>             List of key:value pairs of arguments [none]\n");
        usage.append("  -argfile <arg:argfilename,...> List of argument file names [none]\n");
        usage.append("  -numIterations <num>           Number of iterations to run [1000]\n");
        usage.append("  -minRepeat <min>               Minimum number of times to repeat a run [1]\n");
        usage.append("  -maxRepeat <max>               Maximum number of times to repeat a run [1]\n");
        usage.append("  -rate <tps>                    Query rate in transactions/sec [50000]\n");
        return usage.toString();
    }

    public boolean validateArgs() {
        if (null == host) {
            System.err.println("No host provided.");
            return false;
        }
        if (null == inputFname) {
            System.err.println("No input file provided.");
            return false;
        }

        if (numIterations < 1) {
            System.err.println("numIterations (" + numIterations + ") must be greater than 0.");
            return false;
        }

        if (minRepeat < 1) {
            System.err.println("minRepeat(" + minRepeat + ") must be greater than 0.");
            return false;
        }

        if (maxRepeat < minRepeat) {
            System.err.println("maxRepeat (" + maxRepeat + ") cannot be smaller than minRepeat (" + minRepeat + ").");
            return false;
        }

        if (rate < 1) {
            System.err.println("rate (" + rate + ") must be greater than 0.");
            return false;
        }

        if (!processArgfile()) {
            return false;
        }

        if (null != argstring) {
            String[] pairs = argstring.split(",");
            for (String pair : pairs) {
                String[] kv = pair.split(":");
                if (2 != kv.length) {
                    System.err.println("Bad key-value pair: " + pair);
                    return false;
                }
                argmap.put(kv[0], kv[1]);
            }
        }


        return true;
    }

    private boolean processConfigFile(String fname, Map<String, String> amap)
            throws IOException, FileNotFoundException {
        File cFile = new File(fname);
        if (!cFile.isFile()) {
            System.err.println("Configuration File must be a file");
            return false;
        }

        BufferedReader cReader = new BufferedReader(new FileReader(cFile));
        String line;
        while ((line = cReader.readLine()) != null) {
            String[] fields = line.trim().split("\\s+");
            if (2 != fields.length) {
                System.err.println("Bad line in config file: " + line);
                return false;
            }
            if (null == amap.get(fields[0])) {
                amap.put(fields[0], fields[1]);
            }
        }
        return true;
    }

    public boolean parseArgs(String[] args)
            throws IOException, FileNotFoundException {
        String tkey;
        if (args.length == 0) {
            System.err.println("No arguments specified");
            return false;
        }
        if (0 != args.length % 2)
            return false;

        Map<String, String> amap = new HashMap<String,String>();
        for (int i = 0; i < args.length; i+=2) {
            amap.put(args[i], args[i+1]);
        }

        if (null != (tkey = amap.remove("-configFile")))
            if (!processConfigFile(tkey, amap))
                return false;

        host = amap.remove("-host");
        if (null == host) { // host is required
            System.err.println("Must provide a host");
            return false;
        }

        if (null != (tkey = amap.remove("-port")))                port = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-user")))                username = tkey;
        if (null != (tkey = amap.remove("-pw")))                  password = tkey;
        if (null != (tkey = amap.remove("-ssl-truststore-path"))) truststorePath = tkey;
        if (null != (tkey = amap.remove("-ssl-truststore-pw")))   truststorePwd =  tkey;
        if (null != (tkey = amap.remove("-ssl-keystore-path")))   keystorePath = tkey;
        if (null != (tkey = amap.remove("-ssl-keystore-pw")))     keystorePwd = tkey;
        if (null != (tkey = amap.remove("-f")))                   inputFname = tkey;
        if (null != (tkey = amap.remove("-numThreads")))          numThreads = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-consistencyLevel")))    consistencyLevel = ConsistencyLevel.valueOf(tkey);
        if (null != (tkey = amap.remove("-argfile")))             argfile = tkey;
        if (null != (tkey = amap.remove("-args")))                argstring = tkey;
        if (null != (tkey = amap.remove("-numIterations")))       numIterations = Long.parseLong(tkey);
        if (null != (tkey = amap.remove("-minRepeat")))           minRepeat = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-maxRepeat")))           maxRepeat = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-rate")))                rate = Integer.parseInt(tkey);

        return validateArgs();
    }

    private boolean processArgfile() {
        if (null == argfile)
            return true;
        String[] args = argfile.split(",");
        for (String arg : args) {
            String [] splits = arg.split(":");
            if (2 !=  splits.length) {
                System.err.println("Error: bad argfile argument: " + arg);
                return false;
            }
            System.out.println("  " + splits[0] + " : " + splits[1]);
            if (Files.exists(Paths.get(splits[1]))) {
                argfilemap.put(splits[0], splits[1]);
            }
            else {
                System.err.println("Error: cannot find file " + splits[1]);
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "RainierParams{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", truststorePath='" + truststorePath + '\'' +
                ", truststorePwd='" + truststorePwd + '\'' +
                ", keystorePath='" + keystorePath + '\'' +
                ", keystorePwd='" + keystorePwd + '\'' +
                ", inputFname='" + inputFname + '\'' +
                ", numThreads=" + numThreads +
                ", consistencyLevel=" + consistencyLevel +
                ", argfile='" + argfile + '\'' +
                ", argstring='" + argstring + '\'' +
                ", numIterations=" + numIterations +
                ", minRepeat=" + minRepeat +
                ", maxRepeat=" + maxRepeat +
                ", rate=" + rate +
                '}';
    }
}
