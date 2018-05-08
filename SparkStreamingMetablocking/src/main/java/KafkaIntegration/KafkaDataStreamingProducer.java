package KafkaIntegration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import DataStructures.EntityProfile;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Properties;

public class KafkaDataStreamingProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        //CHOOSE THE INPUT PATH
        String INPUT_PATH = "inputs/dataset2_gp";

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ArrayList<EntityProfile> EntityList = null;
        
		// reading the files
		ObjectInputStream ois;
		try {
			ois = new ObjectInputStream(new FileInputStream(INPUT_PATH));
			EntityList = (ArrayList<EntityProfile>) ois.readObject();
			ois.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        for (EntityProfile entityProfile : EntityList) {
            ProducerRecord<String, String> record = new ProducerRecord<>("mytopic", entityProfile.getStandardFormat());
            producer.send(record);
            Thread.sleep(250);
        }

        producer.close();
    }
}
