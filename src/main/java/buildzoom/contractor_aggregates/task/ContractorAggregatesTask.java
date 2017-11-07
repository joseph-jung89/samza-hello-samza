package buildzoom.contractor_aggregates.task;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

// TODO: make this asynchronous, java threads and callbacks are pretty good, and there might be db call
public class ContractorAggregatesTask implements StreamTask {

	private final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "contractor-permit-aggregated");

	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
		// TODO: define key-value store and start implementing stuff
		int permitId = envelope.getKey();
		// TODO: understand how this would work
		Object contractor_permit_aggregate = envelope.getMessage();



		// after done processing
		OutgoingMessageEnvelope outgoingMessage = new OutgoingMessageEnvelope(
														OUTPUT_STREAM,
														keySerializerName,
														messageSerializerName,
														partitionKey,
														permit_id,
														aggregates
													);
		// fill in outgoing message details
		outgoingMessage.setKey();
		outgoingMessage.setMessage();
		collector.send(outgoingMessage);
	}
}
