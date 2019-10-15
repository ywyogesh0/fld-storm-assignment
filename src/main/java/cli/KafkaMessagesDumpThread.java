package cli;

public class KafkaMessagesDumpThread implements Runnable {

    private MessagesDumpApp messagesDumpApp;

    public KafkaMessagesDumpThread(MessagesDumpApp messagesDumpApp) {
        this.messagesDumpApp = messagesDumpApp;
    }

    @Override
    public void run() {

    }
}
