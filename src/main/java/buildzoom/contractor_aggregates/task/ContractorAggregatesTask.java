package buildzoom.contractor_aggregates.task;

import java.util.HashMap;
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
import buildzoom.contractor_aggregates.model.ContractorAggregatesParser;
import buildzoom.contractor_aggregates.algorithm.ContractorAggregator;

// TODO: make this asynchronous, java threads and callbacks are pretty good, and there might be db call
public class ContractorAggregatesTask implements StreamTask {

	private KeyValueStore<String, HashMap<String, Integer>> store;
	private final String KV_STORE_NAME = "aggregates-db";
	private final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "contractor-permit-aggregated");

	public void init(Config config, TaskContext context) {
		this.store = (KeyValueStore<String, HashMap<String, Integer>>) context.getStore(KV_STORE_NAME);
	}

	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
		// TODO: define key-value store and start implementing stuff
		String permitId = (String) envelope.getKey();
		// TODO: understand how this would work, probably JSON?
		Object contractorPermitMatch = envelope.getMessage();
		HashMap<String, Integer> processedContractorPermitMatch = ContractorAggregatesParser.parseMessage(contractorPermitMatch);
		HashMap<String, Integer> updatedContractorAggregatesForKVStore = ContractorAggregator.calculateAggregates(store.get(permitId), processedContractorPermitMatch);
		// state update for the key-value store
		store.put(permitId, updatedContractorAggregatesForKVStore);

		// after done processing, filter out the intermediates
		HashMap<String, Integer> outputContractorAggregates = ContractorAggregator.filterIntermediates(updatedContractorAggregatesForKVStore);
		OutgoingMessageEnvelope outgoingMessage = new OutgoingMessageEnvelope(
														OUTPUT_STREAM,
														permitId,
														updatedContractorAggregates // json casting?
													);
		// fill in outgoing message details
		collector.send(outgoingMessage);

		// and also import Redis right?
	}
}
