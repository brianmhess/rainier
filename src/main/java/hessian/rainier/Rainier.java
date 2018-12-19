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
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
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

        return RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();
    }

    private void setup()
        throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
               CertificateException, UnrecoverableKeyException  {
        // Connect to Cassandra
        Cluster.Builder clusterBuilder = Cluster.builder()
            .addContactPoint(params.host)
            .withPort(params.port)
            .withLoadBalancingPolicy(new TokenAwarePolicy( DCAwareRoundRobinPolicy.builder().build()));
        if (null != params.username)
            clusterBuilder = clusterBuilder.withCredentials(params.username, params.password);
        if (null != params.truststorePath)
            clusterBuilder = clusterBuilder.withSSL(createSSLOptions());

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

    private void runChain(List<PreparedStatement> stmts, Map<String,String> args, Row row) {
        if (null == stmts)
            return;
        if (stmts.size() < 1)
            return;
        PreparedStatement ps = stmts.get(0);
        String cmd = ps.getQueryString();
        System.out.println("Running: " + cmd);
        List<PreparedStatement> sublist = stmts.subList(1, stmts.size());
        BoundStatement bs = ps.bind();
        Map<String,String> myargs = new HashMap<>(args);
        if (null != row) {
            for (ColumnDefinitions.Definition cdef : row.getColumnDefinitions()) {
                myargs.put(cdef.getName(), codecRegistry.codecFor(cdef.getType()).format(row.getObject(cdef.getName())));
            }
        }
        System.out.println("With variables: " + myargs);
        for (ColumnDefinitions.Definition cdef : ps.getVariables()) {
            if (null == myargs.get(cdef.getName())) {
                System.err.println("Could not find value for key " + cdef.getName());
                System.exit(-1);  // TODO: Maybe do something better here?
            }
            bs.set(cdef.getName(), codecRegistry.codecFor(cdef.getType()).parse(myargs.get(cdef.getName())), codecRegistry.codecFor(cdef.getType()).getJavaType().getRawType());
        }
        List<Row> rows = session.execute(bs).all();
        for (Row r : rows) {
            runChain(sublist, myargs, r);
        }
    }

    private void runIteration(List<PreparedStatement> preparedStatements, Map<String,String> args, Map<String,List<String>> arglistmap,
                              long seed, int minRepeat, int maxRepeat) {
        Random random = new Random(seed);
        Map<String,String> arguments = new HashMap<>(args);
        // Generate random arguments
        for(String k : arglistmap.keySet()) {
            arguments.put(k, arglistmap.get(k).get(random.nextInt(arglistmap.get(k).size())));
        }

        int numRepeat = random.nextInt(maxRepeat - minRepeat + 1) + minRepeat;
        for (int r = 0; r < numRepeat; r++) {
            // Run chain
            runChain(preparedStatements, arguments, null);
        }
    }

    private boolean run(String[] args)
        throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
               CertificateException, UnrecoverableKeyException {
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
        System.err.println("cmds: ");
        cmds.forEach(System.err::println);

        // Prepare queries
        List<PreparedStatement> preparedStatements = new ArrayList<>(cmds.size());
        // TODO: catch preparing exceptions
        for (int i = 0; i < cmds.size(); i++) {
            preparedStatements.add(i, session.prepare(cmds.get(i)));
        }

        // Read argfile arguments
        Map<String,List<String>> arglistmap = new HashMap<>();
        for (String a : params.argfilemap.keySet()) {
            arglistmap.put(a, Files.readAllLines(Paths.get(params.argfilemap.get(a))));
        }

        // Run iterations
        Random random = new Random(0);
        for (long iter = 0; iter < params.numIterations; iter++) {
            runIteration(preparedStatements, params.argmap, arglistmap, iter, params.minRepeat, params.maxRepeat);
        }

        cleanup();

        return true;
    }

    public static void main(String[] args) 
        throws IOException, KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException,
               CertificateException, KeyManagementException {
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

