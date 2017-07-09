import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;

import java.util.ArrayList;
import java.util.List;

public class MqttReplicator implements MqttCallback {
    public final static String REPLICATION_TOPIC = "health_monitor/replication";
    private static final String MONGO_DB = "health_monitor";
    protected MongoClient dbClient;
    protected MongoDatabase db;
    private MqttClient client = null;
    private String MY_IP;

    public MqttReplicator() {
        dbClient = new MongoClient("localhost", new MongoClientOptions.Builder()
                .maxConnectionIdleTime(216000).build());
        db = dbClient.getDatabase(MONGO_DB);
        PropertyFileLoader propertyFileLoader = new PropertyFileLoader();
        MY_IP = propertyFileLoader.getProperty("MY_IP");
        try {
            client = new MqttClient("tcp://"+ propertyFileLoader.getProperty("broker_ip")+":1883", "Sending" + Math.random());
            client.connect();
            client.setCallback(this);
            client.subscribe(REPLICATION_TOPIC , 1);
        } catch (MqttException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        System.out.println("heey");
        MqttReplicator replicator = new MqttReplicator();
    }

    @Override
    public void connectionLost(Throwable throwable) {
        throw new RuntimeException(throwable);
    }

    @Override
    public void messageArrived(String replication_topic, MqttMessage mqttMessage) throws Exception {
        System.out.println("Replication message received ");
        String[] payload = mqttMessage.toString().split("\n");
        String incoming_ip = payload[0].trim();
        if (!incoming_ip.equals(MY_IP)) {
            String topic = payload[1].trim();
            String id = payload[2].trim();
            String[] rows = payload[3].split("%");
            for (int i = 0; i < rows.length; i++) {
                rows[i] = rows[i].trim();
            }
            List<Document> listOfDocuments = new ArrayList<>();
            MongoCollection<Document> collection = db.getCollection(topic + "_" + id);
            for (String row : rows) {
                String[] values = row.split(",");
                listOfDocuments.add(new Document("id", id).append("pressure", values[0]).append("timestamp", Long.valueOf(values[1])));
            }
            collection.insertMany(listOfDocuments);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("PressureSpout.deliveryComplete");
    }
}
