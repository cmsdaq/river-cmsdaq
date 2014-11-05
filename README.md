river-xdaqlas
=============

Playground for elastic search river to CMS DAQ

There are 2 rivers:
    - xdaqlas retrieves all flashlist from the XDAQ LAS and injects
      them into ES.
    - switches gets the switch monitoring JSON and injects the
      information for each switch found in the JSON

The rivers can be created (for DAQ2):
  curl -XPUT localhost:9200/_river/pc-c2e11-18-01:9941/_meta -d '{"type":"xdaqlas","useProxyForLAS":true,"lasURL":"http://pc-c2e11-18-01.cms:9941"}'
  curl -XPUT localhost:9200/_river/pc-c2e11-18-01:9942/_meta -d '{"type":"xdaqlas","useProxyForLAS":true,"lasURL":"http://pc-c2e11-18-01.cms:9942"}'
  curl -XPUT localhost:9200/_river/switches/_meta -d '{"type":"switches","useProxyForLAS":true,"switchesURL":"http://cmsusr1.cms:19343/switches-es-json.jsp"}'

If 'useProxyForLAS' is true, I use 'proxyHost' (localhost) and 'proxyPort' (1080) as SOCKS proxy for requests to the LAS.

The data is inserted in bulk. These parameters can be used to adjust
the bulk insert frequency:
  - bulkActions (default 100) : number of inserts to do at a time
  - concurrentRequests (default 10): number of concurrent requests for inserts
  - flushIntervalSeconds (default 10): maximum time in seconds before performing a new insert
