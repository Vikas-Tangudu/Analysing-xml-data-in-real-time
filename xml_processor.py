import xml.sax
import os
import time

class MetricsHandler(xml.sax.ContentHandler):
    """
    SAX event handler for parsing XML and calculating the mean of metrics
    for each product. The result for each product is saved to Redis.
    """
    def __init__(self, redis_client, filename):
        super().__init__()
        self._redis_client = redis_client
        self._filename = filename
        self._current_tag = ""
        self._prod_id = None
        self._current_metrics = {}
        self._sorted_set_key = "products_by_mean_score"

    def startElement(self, tag, attributes):
        self._current_tag = tag
        if tag == "product":
            self._prod_id = attributes.get("id")
            self._current_metrics = {}

    def characters(self, content):
        content = content.strip()
        if content:
            if self._current_tag in ["metric_x", "metric_y", "metric_z"]:
                try:
                    self._current_metrics[self._current_tag] = float(content)
                except (ValueError, TypeError):
                    print(f"Warning: Non-numeric value found for tag {self._current_tag}. Skipping.")

    def endElement(self, tag):
        self._current_tag = ""
        if tag == "product":
            if self._prod_id and len(self._current_metrics) >= 1:
                try:
                    metric_values = list(self._current_metrics.values())
                    mean_metric = sum(metric_values) / len(metric_values)

                    self._redis_client.delete(self._prod_id) # Being on the safe side.

                    self._redis_client.hmset(self._prod_id, {
                        "mean_metric": mean_metric,
                        "source_file": self._filename
                    })

                    self._redis_client.zadd(self._sorted_set_key, {self._prod_id: mean_metric}) # Adding a Sorted set since it makes querying faster to quickly get products with less than a certain value of mean metric.

                    print(f"Stored mean metric {mean_metric:.2f} for Product ID: {self._prod_id} from file {self._filename} in Redis Hash.")
                except Exception as e:
                    print(f"Error calculating or storing metric for Product ID {self._prod_id}: {e}")

            # Reset the temporary state for the next product tag
            self._prod_id = None
            self._current_metrics = {}

def process_xml_file(filepath, redis_client):
    start_time = time.time()
    try:
        filename = os.path.basename(filepath)
        handler = MetricsHandler(redis_client, filename) # Pass the Redis client and filename to the MetricsHandler for direct access

        xml.sax.parse(filepath, handler) # Sax parses the xml file in stream mode allowing us to work with the data on the fly using handler on the fly.

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Finished processing file '{filename}' in {elapsed_time:.2f} seconds.")

    except Exception as e:
        print(f"Error processing file '{filepath}': {e}")
