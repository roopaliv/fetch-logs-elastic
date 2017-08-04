package logs.data;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Hello world!
 *
 */

public class App {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws UnknownHostException, InterruptedException {
		System.out.println("Hello World!");
		@SuppressWarnings("resource")
		TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(
				new InetSocketTransportAddress(InetAddress.getByName("log.rdu.salab.redhat.com"), 9300));
		SortedMap<String, AliasOrIndex> indexes = client.admin().cluster().prepareState().execute().actionGet()
				.getState().getMetaData().getAliasAndIndexLookup();
		int total = 0;
		for (Entry<String, AliasOrIndex> entry : indexes.entrySet()) {
			String key = entry.getKey();
			AliasOrIndex value = entry.getValue();
			String name = value.getIndices().get(0).getIndex().getName();
			// System.out.println(name);
			int i = 0;
			StringBuilder mainData = new StringBuilder();
			mainData.append("{\"logsdata\": [");
			if (!key.equals(".kibana")) {
				System.out.println(key);
				SearchResponse scrollResp = client.prepareSearch(name).setScroll(new TimeValue(60000))
						.setQuery(QueryBuilders.matchAllQuery()).setSize(100).get(); // max 100 returned per scroll
				// Scroll until no hits are returned
				do {
					for (SearchHit hit : scrollResp.getHits().getHits()) {
						// Handle the hit...
						String json = hit.getSourceAsString();
						mainData.append(json + ",");
						// System.out.println(json);
						i++;
					}

					scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000))
							.execute().actionGet();
				} while (scrollResp.getHits().getHits().length != 0); //end
			}
			
			mainData.setLength(mainData.length() - 1);
			mainData.append("]}");
			String jsonString = mainData.toString();

			JSONObject output;
			try {
				output = new JSONObject(jsonString);
				JSONArray docs = output.getJSONArray("logsdata");
				File file = new File("/home/rovij/Documents/new/zabbixLogs/" + name + ".csv");
				String csv = CDL.toString(docs);
				FileUtils.writeStringToFile(file, csv);
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			total = total + i; 
			System.out.println("file " +name+ " created successfully" + " with " + i + " records");
			/*
			 * SearchRequestBuilder requestBuilder =
			 * client.prepareSearch(name).setQuery(QueryBuilders.matchAllQuery()
			 * ) ; SearchHitIterator hitIterator = new
			 * SearchHitIterator(requestBuilder);
			 * 
			 * while (hitIterator.hasNext()) { //Thread.sleep(200); // so that
			 * the shards do not fail due to overload SearchHit hit =
			 * hitIterator.next(); String json = hit.getSourceAsString();
			 * mainData.append(json + ","); System.out.println(json); }
			 */
		}
		System.out.println("Done with total records: " +total+ "!!!");
	
	}
}
