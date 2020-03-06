import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import ljh.example.Helloworld;
import ljh.example.NotifyGrpc;
import ljh.example.RegisterGrpc;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class GrpcMaster {

    private static Map<String, Tuple2<String, Integer>> workersIpTable = new HashMap<>();

    public static class NotifyClient {
        private final ManagedChannel channel;
        private final NotifyGrpc.NotifyBlockingStub notifyStub;

        public NotifyClient(String executorId){
            String host = workersIpTable.get(executorId)._1;
            int port = workersIpTable.get(executorId)._2;
            channel = ManagedChannelBuilder.forAddress(host,port)
                    .usePlaintext()
                    .build();

            notifyStub = NotifyGrpc.newBlockingStub(channel);
        }

        public void shutdown() throws InterruptedException {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }

        public void notifyToWorker(String message){
            Helloworld.NotifyRequest.Builder reqBuilder = Helloworld.NotifyRequest.newBuilder();
            reqBuilder.setMessage(message);
            Helloworld.Empty empty = notifyStub.notToWorker(reqBuilder.build());
        }
    }

    public static class RegisterServer {
        private static final Logger logger = Logger.getLogger(RegisterServer.class.getName());

        private Server server;
        private int port;

        public RegisterServer(int port) {
            this.port = port;
        }

        public void start() throws IOException {
            server = ServerBuilder.forPort(port)
                    .addService(new RegisterImpl())
                    .build()
                    .start();
            System.err.println("*** RegisterServer started, listening on " + port);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    System.err.println("*** shutting down gRPC server since JVM is shutting down");
                    try {
                        RegisterServer.this.stop();
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

        static class RegisterImpl extends RegisterGrpc.RegisterImplBase{
            @Override
            public void regOnMaster(Helloworld.RegRequest request, StreamObserver<Helloworld.Empty> responseObserver) {
                String workerId = request.getId();
                String workerHost = request.getHost();
                Integer workerPort = request.getPort();
                workersIpTable.put(workerId, new Tuple2<>(workerHost, workerPort));
                responseObserver.onNext(Helloworld.Empty.newBuilder().build());
                responseObserver.onCompleted();
            }
        }
    }

    public static void startRegisterServer(int port){
        new Thread(() -> {
            final RegisterServer server = new RegisterServer(port);
            try {
                server.start();
                server.blockUntilShutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }).start();
    }

    public static void notifyToAllWorkers(){
        for (String key: workersIpTable.keySet()) {
            if (!key.equals("driver")){
                GrpcMaster.NotifyClient nc = new GrpcMaster.NotifyClient(key);
                nc.notifyToWorker("Master has registered worker: " + key);
            }
        }
    }
}
