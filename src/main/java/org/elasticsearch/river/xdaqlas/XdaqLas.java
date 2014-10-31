package org.elasticsearch.river.xdaqlas;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.unit.TimeValue;


public class XdaqLas extends AbstractRiverComponent implements River {

  private final Client client;
  public String lasURL;
  private volatile Thread thread;
  protected volatile BulkProcessor bulkProcessor;

  @SuppressWarnings({"unchecked"})
  @Inject
  public XdaqLas(RiverName riverName, RiverSettings settings, Client client) {
    super(riverName, settings);
    this.client = client;

    //RunRiver Settings
    Map<String, Object> rSettings = settings.settings();
    //lasURL = XContentMapValues.nodeStringValue(rSettings.get("lasURL"), "http://dvsrv-c2f36-09-01.cms:9941/urn:xdaq-application:service=xmaslas2g");
    lasURL = XContentMapValues.nodeStringValue(rSettings.get("lasURL"), "http://pc-c2e11-18-01.cms:9941/urn:xdaq-application:service=xmaslas2g");

    // Creating bulk processor
    this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
          logger.debug("Going to execute new bulk composed of {} actions", request.numberOfActions());
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
          logger.debug("Executed bulk composed of {} actions", request.numberOfActions());
          if (response.hasFailures()) {
            logger.warn("There was failures while executing bulk", response.buildFailureMessage());
            if (logger.isDebugEnabled()) {
              for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed()) {
                  logger.debug("Error for {}/{}/{} for {} operation: {}", item.getIndex(),
                               item.getType(), item.getId(), item.getOpType(), item.getFailureMessage());
                }
              }
            }
          }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
          logger.warn("Error executing bulk", failure);
        }
      })
      .setBulkActions(100)
      .setConcurrentRequests(10)
      .setFlushInterval(TimeValue.timeValueSeconds(10))
      .build();
  }

  @Override
  public void start() {
    logger.info("Start retrieving data from "+lasURL);
    thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "las_slurper").newThread(new Slurper(lasURL));
    thread.start();
  }

  @Override
  public void close() {
    if (thread != null) {
      thread.interrupt();
    }
    if (this.bulkProcessor != null) {
      this.bulkProcessor.close();
    }
    logger.info("Stopped retrieving data from "+lasURL);
  }

  private class Slurper implements Runnable {
    private final String lasURL;
    private final JSONParser parser;
    private final DateFormat dateFormat;

    private Slurper(String lasURL) {
      this.lasURL = lasURL;
      this.parser = new JSONParser();
      this.dateFormat = new SimpleDateFormat("E, MMM dd yyyy HH:mm:ss z");

      Properties sysProperties = System.getProperties();
      sysProperties.put("socksProxyHost", "localhost");
      sysProperties.put("socksProxyPort", "1080");
      sysProperties.put("proxySet",  "true");
    }

    @Override
    public void run() {
      HttpURLConnection conn = null;
      InputStream in = null;

      try {
        while(true) {
          logger.info("Getting data from "+lasURL);
          URL url = null;
          try {
            url = new URL(lasURL+"/retrieveCollection?flash=urn:xdaq-flashlist:BU&fmt=json");
          }
          catch (java.net.MalformedURLException e) {
            logger.error("Bad LAS URL: "+e.getMessage());
            continue;
          }
          JSONArray array;
          try {
            conn = (HttpURLConnection) url.openConnection();
            in = conn.getInputStream();
            InputStreamReader isr = new InputStreamReader(in);
            BufferedReader reader = new BufferedReader(isr);
            String rawJSON = reader.readLine();
            JSONObject jsonObject = (JSONObject)this.parser.parse(rawJSON);
            JSONObject table = (JSONObject)jsonObject.get("table");
            //System.out.println(table.get("rows"));
            String indexName = "flashlist";
            String typeName = "BU";
            JSONArray rows=(JSONArray)table.get("rows");
            Iterator it = rows.iterator();
            while (it.hasNext())
            {
              JSONObject row = (JSONObject)it.next();
              //System.out.println(row);
              String id = (String)row.get("instance");
              try {
                Date timestamp = this.dateFormat.parse((String)row.get("timestamp"));
                bulkProcessor.add(
                  Requests.indexRequest(indexName)
                  .type(typeName)
                  .id(id)
                  .timestamp(String.valueOf(timestamp.getTime()))
                  .source(row.toString())
                );
              }
              catch (java.text.ParseException e) {
                logger.error("Failed to parse timestamp "+row.get("timestamp"));
              }
            }
          }
          catch (IOException e) {
            logger.error("Error retrieving flash list from URL "+lasURL+": "+e.getMessage());
          }
          catch (org.json.simple.parser.ParseException e) {
            logger.error("Could not parse response from "+lasURL+": "+e.getMessage());
          }

          Thread.sleep(10000);
        }
      }
      catch (java.lang.InterruptedException e)
      {}
      finally {
        try
        {
          if (in != null)
            in.close();
          if (conn != null)
            conn.disconnect();
        }
        catch (java.io.IOException e)
        {}
      }
    }
  }
}
