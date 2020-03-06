import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class GrpcBuilder {
    public static void main(String[] args) throws UnknownHostException {
        SparkConf conf = new SparkConf().setAppName("Grpc Application");

        JavaSparkContext sc = new JavaSparkContext(conf);

        int executorNum = sc.getConf().getInt("spark.executor.instances", 1);

        String masterIp = InetAddress.getLocalHost().getHostAddress();
        System.err.println("spark.executor.instances: " + executorNum);
        System.err.println("masterIp: " + masterIp);

        int masterPort = 50000 + (int) Thread.currentThread().getId() % 10000;
        Broadcast<String> bcIp = sc.broadcast(masterIp);
        Broadcast<Integer> bcPort = sc.broadcast(masterPort);

        GrpcMaster.startRegisterServer(masterPort);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<Integer> list = Arrays.asList(new Integer[executorNum]);
        sc.parallelize(list).repartition(executorNum).foreachPartition(
                (VoidFunction<Iterator<Integer>>) integerIterator -> {
                    String regHost = bcIp.getValue();
                    int regPort = bcPort.getValue();
                    String id = SparkEnv.get().executorId();
                    String workerHost = InetAddress.getLocalHost().getHostAddress();
                    int workerPort = 50000 + (int) Thread.currentThread().getId() % 10000;
                    GrpcWorker.RegisterClient cl = GrpcWorker.RegisterClient.getInstance(regHost, regPort);
                    cl.regOnMaster(id, workerHost, workerPort);
                    GrpcWorker.startNotifyServer(workerPort);
                }
        );

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        GrpcMaster.notifyToAllWorkers();

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        sc.stop();
        System.exit(0);
    }


}
