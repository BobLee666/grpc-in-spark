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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class GrpcWorker {

    public static class RegisterClient {
        private static RegisterClient instance;
        private final ManagedChannel channel;
        private final RegisterGrpc.RegisterBlockingStub registerStub;

        private RegisterClient(String host, int port){
            channel = ManagedChannelBuilder.forAddress(host,port)
                    .usePlaintext()
                    .build();

            registerStub = RegisterGrpc.newBlockingStub(channel);
        }

        public static synchronized RegisterClient getInstance(String host, int port) {
            if (instance == null) {
                instance = new RegisterClient(host, port);
            }
            return instance;
        }
        public void shutdown() throws InterruptedException {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }

        public void regOnMaster(String id, String host, int port){
            RegRequest.Builder reqBuilder = RegRequest.newBuilder();
            reqBuilder.setId(id);
            reqBuilder.setHost(host);
            reqBuilder.setPort(port);
            Empty empty = registerStub.regOnMaster(reqBuilder.build());
        }
    }

    public static class NotifyServer {
        private static NotifyServer instance;
        private Server server;
        private int port;

        private NotifyServer(int port) {
            this.port = port;
        }

        public static synchronized NotifyServer getInstance(int port) {
            if (instance == null) {
                instance = new NotifyServer(port);
            }
            return instance;
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
            final NotifyServer server = NotifyServer.getInstance(port);
            try {
                server.start();
                server.blockUntilShutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }).start();
    }
}
