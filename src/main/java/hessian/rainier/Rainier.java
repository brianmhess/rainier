/*
 * Copyright 2015 Brian Hess
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hessian.rainier;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.PercentileSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;


import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.*;

public class Rainier {
    private static String version = "0.0.1";

    private RainierParams params = new RainierParams();

    private Cluster cluster = null;
    private RateLimitedSession session = null;
    private CodecRegistry codecRegistry = null;

    private String usage() {
        return "version: " + version + "\n"
                + RainierParams.usage();
    }

    private SSLOptions createSSLOptions()
        throws KeyStoreException, IOException, NoSuchAlgorithmException,
            KeyManagementException, CertificateException, UnrecoverableKeyException {
        TrustManagerFactory tmf = null;
        if (null != params.truststorePath) {
            KeyStore tks = KeyStore.getInstance("JKS");
            tks.load(new FileInputStream(new File(params.truststorePath)),
                    params.truststorePwd.toCharArray());
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(tks);
        }

        KeyManagerFactory kmf = null;
        if (null != params.keystorePath) {
            KeyStore kks = KeyStore.getInstance("JKS");
            kks.load(new FileInputStream(new File(params.keystorePath)),
                    params.keystorePwd.toCharArray());
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(kks, params.keystorePwd.toCharArray());
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf != null? kmf.getKeyManagers() : null,
                        tmf != null ? tmf.getTrustManagers() : null,
                        new SecureRandom());

        RemoteEndpointAwareJdkSSLOptions.Builder sslOptionsBuilder = RemoteEndpointAwareJdkSSLOptions.builder();
        sslOptionsBuilder.withSSLContext(sslContext);
        //sslOptionsBuilder.withCipherSuites(new String[]{"TLS_RSA_WITH_AES_128_CBC_SHA"});  // F
        return sslOptionsBuilder.build();
    }

    private void setup()
        throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
               CertificateException, UnrecoverableKeyException  {
        // Connect to Cassandra
        Cluster.Builder clusterBuilder = Cluster.builder()
            .addContactPoint(params.host)
            .withPort(params.port);
        if (null != params.username)
            clusterBuilder = clusterBuilder.withCredentials(params.username, params.password);
        if (null != params.truststorePath)
            clusterBuilder = clusterBuilder.withSSL(createSSLOptions());

        // Speculative Retry Policy
        //clusterBuilder.withSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(500,2)); //F
        //clusterBuilder.withSpeculativeExecutionPolicy(new PercentileSpeculativeExecutionPolicy(ClusterWidePercentileTracker.builder(6000).build(), 99.0, 2)) //F

        // Socket Options
        SocketOptions socketOptions = new SocketOptions();
        //socketOptions.setConnectTimeoutMillis(5000); // F
        //socketOptions.setReadTimeoutMillis(12000); // F
        clusterBuilder.withSocketOptions(socketOptions);

        // Load Balancing Policy
        clusterBuilder.withLoadBalancingPolicy(new TokenAwarePolicy( DCAwareRoundRobinPolicy.builder().build()));

        // Query Options
        QueryOptions queryOptions = new QueryOptions();
        //queryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM); // F
        //queryOptions.setDefaultIdempotence(true); // F
        clusterBuilder.withQueryOptions(queryOptions);

        cluster = clusterBuilder.build();
        if (null == cluster) {
            throw new IOException("Could not create cluster");
        }
        Session tsession = cluster.connect();
        RateLimiter rateLimiter = new RateLimiter(params.rate);
        session = new RateLimitedSession(tsession, rateLimiter);
        codecRegistry = cluster.getConfiguration().getCodecRegistry();
    }

    private void cleanup() {
        if (null != session)
            session.close();
        if (null != cluster)
            cluster.close();
    }

    private boolean run(String[] args)
        throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
               CertificateException, UnrecoverableKeyException, InterruptedException, ExecutionException {
        // process arguments
        if (!params.parseArgs(args)) {
            System.err.println("Error processing arguments\n" + usage());
            return false;
        }
        System.err.println("Params: " + params.toString());

        // setup
        setup();

        // Read input file
        List<String> cmds = Files.readAllLines(Paths.get(params.inputFname));

        // Prepare queries
        List<PreparedStatement> preparedStatements = new ArrayList<>(cmds.size());
        // TODO: catch preparing exceptions
        System.err.println("cmds: ");
        for (int i = 0; i < cmds.size(); i++) {
            if (cmds.get(i).length() == 0) continue; // skip empty lines
            if (cmds.get(i).startsWith("#")) continue; // skip lines that start with # (for commenting)
            System.err.println(cmds.get(i));
            preparedStatements.add(i, session.prepare(cmds.get(i)));
        }

        // Read argfile arguments
        Map<String,List<String>> arglistmap = new HashMap<>();
        for (String a : params.argfilemap.keySet()) {
            arglistmap.put(a, Files.readAllLines(Paths.get(params.argfilemap.get(a))));
        }

        // Single Threaded
        if (1 == params.numThreads) {
            // Run iterations
            Random random = new Random(0);
            RainierTask rainierTask = new RainierTask(session, codecRegistry, preparedStatements, params.argmap, arglistmap, 0, params.minRepeat, params.maxRepeat, 0);
            for (long iter = 0; iter < params.numIterations; iter++) {
                rainierTask.runIteration( preparedStatements, params.argmap, arglistmap, iter, params.minRepeat, params.maxRepeat, session, codecRegistry, 0L);
            }
        }
        // Multi-Threaded
        else {
            ExecutorService executor = Executors.newFixedThreadPool(params.numThreads);
            Set<Future<Long>> results = new HashSet<>();
            for (long iter = 0; iter < params.numIterations; iter++) {
                Callable<Long> worker = new RainierTask(session, codecRegistry, preparedStatements, params.argmap, arglistmap, iter, params.minRepeat, params.maxRepeat, iter);
                results.add(executor.submit(worker));
            }
            executor.shutdown();
            long total = 0;
            for (Future<Long> res : results) {
                total += res.get();
            }
            System.out.println("Completed " + params.numIterations + " iterations, for a total of " + total + " total chains");
        }

        cleanup();

        return true;
    }

    public static void main(String[] args) 
        throws IOException, KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException,
               CertificateException, KeyManagementException, InterruptedException, ExecutionException {
        Rainier rainier = new Rainier();
        boolean success = rainier.run(args);
        if (success) {
            System.exit(0);
        } else {
            System.err.println("There was an error");
            System.exit(-1);
        }
    }
}

