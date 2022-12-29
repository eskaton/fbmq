package ch.eskaton.fbmq;

import ch.eskaton.fbmq.impl.FBMQBroker;
import ch.eskaton.fbmq.impl.FBMQConnectionFactory;
import org.apache.log4j.BasicConfigurator;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class TestBroker {

    @Test
    public void test() throws FBMQException, IllegalStateException, IOException {
        BasicConfigurator.configure();

        var broker = new FBMQBroker();

        broker.start();

        var outputPath = "output";
        var factory = new FBMQConnectionFactory();
        var conn = factory.createConnection("vm://fbmq");
        var session = conn.createSession();
        var testQueue = session.createQueue("test");
        var sender = session.createProducer(testQueue);
        var outputDir = new File(outputPath);

        if (!outputDir.exists() && !outputDir.mkdir()) {
            throw new RuntimeException("Failed to create directory " + outputDir.getName());
        }

        long begin = System.currentTimeMillis();
        long end;

        for (int i = 0; i < 1000; i++) {
            sender.send(session.createByteMessage(("Test" + i).getBytes()));
        }

        session.commit();
        session.close();

        var receiver1 = new Thread(new Receiver(conn));
        var receiver2 = new Thread(new Receiver(conn));

        receiver1.start();
        receiver2.start();

        try {
            receiver1.join();
            receiver2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        end = System.currentTimeMillis();

        System.out.println("Elapsed time: " + (end - begin) + "ms");

        conn.close();
        broker.stop();

        Files.walk(Path.of(outputPath))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    static class Receiver implements Runnable {

        private FBMQConnection conn;

        public Receiver(FBMQConnection conn) {
            this.conn = conn;
        }

        public void run() {
            FBMQSession session = null;

            try {
                session = conn.createSession();

                var testQueue = session.createQueue("test");
                var receiver = session.createConsumer(testQueue);

                FBMQMessage message;

                int count = 0;

                while ((message = receiver.receive()) != null) {
                    count++;

                    writeFile("output/test" + Thread.currentThread().getId() + "-" + count + ".dat", message.getBody());

                    session.commit();
                }

                System.out.println(Thread.currentThread().getName() + ": " + count + " messages received");
            } catch (Exception e) {
                e.printStackTrace();

                try {
                    if (session != null) {
                        session.rollback();
                    }
                } catch (Exception e1) {
                }
            }
        }
    }

    static byte[] slurp(String fileName) throws IOException {
        try (var file = new FileInputStream(fileName)) {
            var channel = file.getChannel();
            var readBuffer = channel.map(MapMode.READ_ONLY, 0L, channel.size());
            var buf = new byte[(int) channel.size()];

            readBuffer.get(buf);

            return buf;
        }
    }

    static void writeFile(String fileName, byte[] buf) throws IOException {
        try (var raf = new RandomAccessFile(fileName, "rw")) {
            var channel = raf.getChannel();
            var writeBuffer = channel.map(MapMode.READ_WRITE, 0, buf.length);

            writeBuffer.put(buf);
        }
    }

}
