package PrimeApproach;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {

    private final static String TOPIC = "outputtopic";
    private static String BOOTSTRAP_SERVERS;
    private static String OUTPUT_PATH;
    private static int windowPoll;
    private static int timeDeadline;
    
    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                                    "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer =
                                    new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }
    
    static void runConsumer() throws InterruptedException, IOException {
        final Consumer<Long, String> consumer = createConsumer();

        int noRecordsCount = 0;
        long initialTime = System.currentTimeMillis();
        int increment = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(windowPoll);
            if (System.currentTimeMillis() - initialTime > timeDeadline && consumerRecords.count()==0) {
            	initialTime = System.currentTimeMillis();
            	increment++;
			}
            
//            if (consumerRecords.count()!=0) { //syncronize with each increment
//            	initialTime = System.currentTimeMillis();
//            }

//            if (consumerRecords.count()==0) {
//                noRecordsCount++;
//                if (noRecordsCount > giveUp) {
//                	break;
//                }
//                else {
//                	Thread.sleep(1000);
//                	continue;
//                }
//            } else {
//            	giveUp = 0;
//            }
            
            if (consumerRecords.count()!=0) {
            	File arquivo = new File(OUTPUT_PATH.replace("$$", String.valueOf(increment)));
                FileOutputStream output;
        		output = new FileOutputStream(arquivo, true);//true to append in the end of the file
        		BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(output));
        		            
                consumerRecords.forEach(record -> {
                	
                    try {
    					bufferedWriter.write(record.value() + "\n");
    				} catch (IOException e) {
    					e.printStackTrace();
    				}
//                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
//                            record.key(), record.value(),
//                            record.partition(), record.offset());
                });
                
                bufferedWriter.flush();
                output.close();
                bufferedWriter.close();
            }
            

            consumer.commitAsync();
        }
//        consumer.close();
//        System.out.println("DONE");
    }
    
    public static void main(String... args) throws Exception {
    	BOOTSTRAP_SERVERS = args[0];
    	OUTPUT_PATH = args[1];
    	windowPoll = Integer.parseInt(args[2]);
    	timeDeadline = Integer.parseInt(args[3])*1000; //seconds to milliseconds
        runConsumer();
    }
}
