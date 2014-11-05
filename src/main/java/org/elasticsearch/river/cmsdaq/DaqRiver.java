package org.elasticsearch.river.cmsdaq;

import java.util.Map;

import java.net.Proxy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;


public abstract class DaqRiver extends AbstractRiverComponent {

  protected final Proxy lasProxy;
  protected volatile BulkProcessor bulkProcessor;

  @SuppressWarnings({"unchecked"})
  @Inject
  public DaqRiver(RiverName riverName, RiverSettings settings, Client client) {
    super(riverName, settings);

    Map<String, Object> rSettings = settings.settings();

    // Create SOCKS proxy if requested
    final Boolean useProxy = XContentMapValues.nodeBooleanValue(rSettings.get("useProxyForLAS"),false);
    if (useProxy) {
      final String hostname = XContentMapValues.nodeStringValue(rSettings.get("proxyHost"), "localhost");
      final Integer port = XContentMapValues.nodeIntegerValue(rSettings.get("proxyPort"), 1080);
      logger.info("Using SOCKS proxy "+hostname+":"+port);
      SocketAddress addr = new
        InetSocketAddress(hostname, port);
      lasProxy = new Proxy(Proxy.Type.SOCKS, addr);
    }
    else {
      lasProxy = null;
    }

    // Creating bulk processor
    final Integer bulkActions = XContentMapValues.nodeIntegerValue(rSettings.get("bulkActions"), 100);
    final Integer concurrentRequests = XContentMapValues.nodeIntegerValue(rSettings.get("concurrentRequests"), 10);
    final Integer flushIntervalSeconds = XContentMapValues.nodeIntegerValue(rSettings.get("flushIntervalSeconds"), 10);

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
      .setBulkActions(bulkActions)
      .setConcurrentRequests(concurrentRequests)
      .setFlushInterval(TimeValue.timeValueSeconds(flushIntervalSeconds))
      .build();
  }
}
