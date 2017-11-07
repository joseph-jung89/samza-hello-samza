package buildzoom.contractor_aggregates.algorithm;

import javax.json.JsonObject;
import javax.json.JsonParser;

public class ContractorAggregator {

	private final String NUMBER_OF_PERMIT_KEY = "count";
	private final String TOTAL_JOB_VALUE_KEY = "total_job_value";
	private final String MEDIAN_JOB_VALUE_KEY = "median_job_value";
	private final String MAX_JOB_VALUE_KEY = "max_job_value";
	private final String MIN_JOB_VALUE_KEY = "min_job_value";
	private final String JOB_VALUE_DATA_KEY = "job_values";
	private final String NUM_PERMITS_UNPRICED_KEY = "num_permits_unpriced";
	private final String EARLIEST_PERMIT_DATE_KEY = "earliest_permit_date";
	private final String LATEST_PERMIT_DATE_KEY = "latest_permit_date";

	public static JsonObject calculateAggregates(
		JsonObject storedIntermediateMap, JsonObject processedContractorPermitMatch) {

		return new JsonObject();

	}

	public static JsonObject filterIntermediates(JsonObject intermediateMap) {

		return new JsonObject();
	}
}