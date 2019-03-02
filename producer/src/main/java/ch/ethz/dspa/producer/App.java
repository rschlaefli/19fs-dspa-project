package ch.ethz.dspa.producer;

import java.time.Duration;


public class App 
{
    public static void main( String[] args )
    {
    	// TODO [nku] add argument parsing
    	
        DSPAProducer producer = new DSPAProducer()
        								.withWorker(4)
        								.withFile("./../data/1k-users-sorted/streams/post_event_stream.csv")
        								.withTopic("post")
        								.withSpeedup(Duration.ofSeconds(60).toMillis())
        								.withMaxDelay(Duration.ofMinutes(1))
        								.withMaxRandomDelay(Duration.ofSeconds(20))
        								.withSeed(1)
        								.create();
        
        producer.start();

    }
}
