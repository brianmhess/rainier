package hessian.rainier;

import com.datastax.driver.core.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

public class RainierTask implements Callable<Long> {
    private Session session = null;
    private CodecRegistry codecRegistry = null;
    private List<PreparedStatement> preparedStatements = null;
    private Map<String,String> arguments = null;
    private Map<String,List<String>> arglistmap = null;
    private long seed = 0;
    private int minRepeat = 1;
    private int maxRepeat = 1;
    private long taskNum = 0;
    private Random random = null;

    public RainierTask(Session session, CodecRegistry codecRegistry, List<PreparedStatement> preparedStatements,
                       Map<String, String> arguments, Map<String, List<String>> arglistmap, long seed, int minRepeat,
                       int maxRepeat, long taskNum) {
        this.session = session;
        this.codecRegistry = codecRegistry;
        this.preparedStatements = preparedStatements;
        this.arguments = arguments;
        this.arglistmap = arglistmap;
        this.seed = seed;
        this.minRepeat = minRepeat;
        this.maxRepeat = maxRepeat;
        this.taskNum = taskNum;
        random = new Random(this.seed);
    }

    public Long call() {
        return (long)runIteration(preparedStatements, arguments, arglistmap, seed, minRepeat, maxRepeat, session, codecRegistry, taskNum);
    }

    public void runChain(List<PreparedStatement> stmts, Map<String,String> args, Row row, Session session,
                                CodecRegistry codecRegistry, long taskNum) {
        if (null == stmts)
            return;
        if (stmts.size() < 1)
            return;
        PreparedStatement ps = stmts.get(0);
        String cmd = ps.getQueryString();
        System.out.println(String.format("[%5d] Running: %s", taskNum, cmd));
        List<PreparedStatement> sublist = stmts.subList(1, stmts.size());
        BoundStatement bs = ps.bind();
        Map<String,String> myargs = new HashMap<>(args);
        if (null != row) {
            for (ColumnDefinitions.Definition cdef : row.getColumnDefinitions()) {
                myargs.put(cdef.getName(), codecRegistry.codecFor(cdef.getType()).format(row.getObject(cdef.getName())));
            }
        }
        System.out.println(String.format("[%5d] With variables: %s", taskNum, myargs));
        for (ColumnDefinitions.Definition cdef : ps.getVariables()) {
            if (null == myargs.get(cdef.getName())) {
                System.err.println(String.format("[%5d] Could not find value for key %s", taskNum, cdef.getName()));
                System.exit(-1);  // TODO: Maybe do something better here?
            }
            bs.set(cdef.getName(), codecRegistry.codecFor(cdef.getType()).parse(myargs.get(cdef.getName())), codecRegistry.codecFor(cdef.getType()).getJavaType().getRawType());
        }
        List<Row> rows = session.execute(bs).all();
        for (Row r : rows) {
            runChain(sublist, myargs, r, session, codecRegistry, taskNum);
        }
    }

    public  int runIteration(List<PreparedStatement> preparedStatements, Map<String,String> args, Map<String,List<String>> arglistmap,
                              long seed, int minRepeat, int maxRepeat, Session session, CodecRegistry codecRegistry, long taskNum) {
        Map<String,String> arguments = new HashMap<>(args);
        // Generate random arguments
        for(String k : arglistmap.keySet()) {
            arguments.put(k, arglistmap.get(k).get(random.nextInt(arglistmap.get(k).size())));
        }

        int numRepeat = random.nextInt(maxRepeat - minRepeat + 1) + minRepeat;
        for (int r = 0; r < numRepeat; r++) {
            // Run chain
            System.out.println(String.format("\n[%5d] Iter %d repeat %d", taskNum, seed, r));
            runChain(preparedStatements, arguments, null, session, codecRegistry, taskNum);
        }
        return numRepeat;
    }
}
