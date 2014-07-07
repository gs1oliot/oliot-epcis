// THIS CQL Script creates EPCIS Keyspace for Cassandra
// Run: ./cqlsh - f <ScriptFileName>

DROP KEYSPACE SSC1;

CREATE KEYSPACE SSC1 
WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE SSC1;
CREATE TABLE objectevent (
  epc text,
  eventtime timestamp,
  action text,
  bizlocation text,
  bizstep text,
  biztransaction map<text, text>,
  disposition text,
  epcset set<text>,  
  prefix map<text, text>,
  readpoint text,
  recordtime timestamp,
  timezone text,
  PRIMARY KEY (epc, eventtime)  
) WITH CLUSTERING ORDER BY (eventtime DESC);
CREATE INDEX ON objectevent(action);
CREATE INDEX ON objectevent(bizlocation);
CREATE INDEX ON objectevent(bizstep);

CREATE TABLE quantityevent (
  epcclass text,
  eventtime timestamp,
  bizlocation text,
  bizstep text,
  biztransaction map<text, text>,
  disposition text,
  prefix map<text, text>,
  quantity int,
  readpoint text,
  recordtime timestamp,
  timezone text,
  PRIMARY KEY (epcclass, eventtime)
);
CREATE INDEX ON quantityevent(bizlocation);
CREATE INDEX ON quantityevent(bizstep);

CREATE TABLE aggregationevent (
  parentid text,
  eventtime timestamp,
  action text,
  bizlocation text,
  bizstep text,
  biztransaction map<text, text>,
  disposition text,
  epcset set<text>,
  prefix map<text, text>,
  readpoint text,
  recordtime timestamp,
  timezone text,
  PRIMARY KEY (parentid, eventtime)
)WITH CLUSTERING ORDER BY (eventtime DESC);
CREATE INDEX ON aggregationevent(action);
CREATE INDEX ON aggregationevent(bizlocation);
CREATE INDEX ON aggregationevent(bizstep);

CREATE TABLE EPCIndex_AggregationEvent(
	childepc text,
	eventtime timestamp,
	parentid text,
	PRIMARY KEY (childepc, eventtime, parentid)
)WITH CLUSTERING ORDER BY (eventtime DESC);

CREATE TABLE transactionevent (
  epc text,
  eventtime timestamp,
  bizlocation text,
  bizstep text,
  biztransaction map<text, text>,
  disposition text,
  epcset set<text>,
  prefix map<text, text>,
  readpoint text,
  recordtime timestamp,
  timezone text,
  PRIMARY KEY (epc, eventtime)
);
CREATE INDEX ON transactionevent(bizlocation);
CREATE INDEX ON transactionevent(bizstep);

CREATE TABLE masterdata (
  itemkey text PRIMARY KEY,
  urn_epcglobal_fmcg_mda_address text,
  urn_epcglobal_fmcg_mda_compartment text,
  urn_epcglobal_fmcg_mda_expire text,
  urn_epcglobal_fmcg_mda_latitude text,
  urn_epcglobal_fmcg_mda_longitude text,
  urn_epcglobal_fmcg_mda_name text,
  vocabtype text
);
CREATE INDEX ON masterdata(vocabtype);

CREATE TABLE Subscription (
	subscriptionid text PRIMARY KEY,
	params blob,
	dest text,
	sched blob,
	trigg text,
	initialrecordingtime timestamp,
	exportifempty boolean,
	queryname text,
	lastexecuted timestamp
);
	