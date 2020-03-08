import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import ljh.example.Helloworld;
import ljh.example.Helloworld.Empty;
import ljh.example.Helloworld.NotifyRequest;
import ljh.example.Helloworld.RegRequest;
import ljh.example.NotifyGrpc;
import ljh.example.RegisterGrpc;
import org.apache.spark.SparkEnv;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

public class GrpcWorker {

    private static GrpcWorker instance;
    private final ManagedChannel regChannel;
    private final RegisterGrpc.RegisterBlockingStub registerStub;
    private static String workerHost;
    private static int workerPort;
    private static String executorId;

    private GrpcWorker(String host, int port) {
        try {
            workerHost = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        workerPort = 50000 + (int) Thread.currentThread().getId() % 10000;
        executorId = SparkEnv.get().executorId();
        regChannel = ManagedChannelBuilder.forAddress(host,port)
                .usePlaintext()
                .build();

        registerStub = RegisterGrpc.newBlockingStub(regChannel);

        startNotifyServer(workerPort);

    }

    public static GrpcWorker getInstance(String host, int port) {
        if (instance != null) return instance;
        synchronized (GrpcWorker.class){
            if (instance == null){
                instance = new GrpcWorker(host, port);
            }
        }
        return instance;
    }

    public void shutdown() throws InterruptedException {
        regChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void regOnMaster(){
        RegRequest.Builder reqBuilder = RegRequest.newBuilder();
        reqBuilder.setId(executorId);
        reqBuilder.setHost(workerHost);
        reqBuilder.setPort(workerPort);
        Empty empty = registerStub.regOnMaster(reqBuilder.build());
    }

    public static class NotifyServer {
        private Server server;
        private int port;

        private NotifyServer(int port) {
            this.port = port;
        }

        public void start() throws IOException {
            server = ServerBuilder.forPort(port)
                    .addService(new NotifyServer.NotifyImpl())
                    .build()
                    .start();
            System.err.println("*** NotifyServer started, listening on " + port);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    System.err.println("*** shutting down gRPC server since JVM is shutting down");
                    try {
                        NotifyServer.this.stop();
                    } catch (InterruptedException e) {
                        e.printStackTrace(System.err);
                    }
                    System.err.println("*** server shut down");
                }
            });
        }

        private void stop() throws InterruptedException {
            if (server != null) {
                server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            }
        }

        /**
         * Await termination on the main thread since the grpc library uses daemon threads.
         */
        public void blockUntilShutdown() throws InterruptedException {
            if (server != null) {
                server.awaitTermination();
            }
        }

        static class NotifyImpl extends NotifyGrpc.NotifyImplBase{
            @Override
            public void notToWorker(NotifyRequest request, StreamObserver<Empty> responseObserver) {
                System.err.println("worker successfully receive master reply: " + request.getMessage());
                responseObserver.onNext(Helloworld.Empty.newBuilder().build());
                responseObserver.onCompleted();
            }
        }
    }

    public static void startNotifyServer(int port){
        new Thread(() -> {
            final NotifyServer server = new NotifyServer(port);
            try {
                server.start();
                server.blockUntilShutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }).start();
    }
}
