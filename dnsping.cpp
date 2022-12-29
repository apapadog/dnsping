/* 
 * dnsping - Tracking DNS Performance to top sites
 * by Antonis Papadogiannakis <papadog@ics.forth.gr>
 *
 */

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <signal.h>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <sstream>
#include <ldns/ldns.h>
#include <mysql++.h>

using namespace std;


//default parameters
int verbose=0;

string *domains=NULL;

void Usage(char *pname) {
	printf("Usage: %s ",pname);
	printf("[-f filename] [-n number] [-i interval] [-c count]\n");
	printf("\t[-u DBuser] [-p DBpassword] [-s DBserver] [-P DBport] [-d DBname] [Chv]\n\n");

	printf(" [-f filename] \tfilename with top domains (default: top-domains.csv)\n");
	printf(" [-n number] \tnumber of top domains (default: 10)\n");
	printf(" [-i interval] \tseconds between successive queries (default: 60 seconds)\n");
	printf(" [-c count] \tnumber of queries for each domain (default: infinite)\n");
	printf(" [-u DB user] \tusername for mysql database (default: papadog)\n");
	printf(" [-p DBpasswd] \tpassword for mysql database (default: ******)\n");
	printf(" [-s DBserver] \tmysql server (default: localhost)\n");
	printf(" [-P DBport] \tmysql server port (default: none)\n");
	printf(" [-d DBname] \tdatabase name (default: dnsping)\n");
	printf(" [-C] \t\tClear Database tables\n");
	printf(" [-v] \t\tverbose\n");
	printf(" [-h] \t\thelp\n");
}

//handle signals
void terminate(int signo) {

	if (domains!=NULL) 
		delete [] domains;
	domains=NULL;
	printf("Exiting\n");
	exit(1);
}

//probe a domain's nameserver
int probe(string domain, uint32_t *latency, struct timeval *timestamp) {
	ldns_resolver *resolver=NULL;	//local dns resolver to use for queries
	ldns_rdf *rname=NULL;		//name to query for
	ldns_pkt *p=NULL;		//dns packet
	ldns_rr_list *a=NULL;		//A records from answer
	ldns_status s;			//error checking
	//struct timeval start, end;

	static string charset="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	string name;			//hostname=random string.domain

	//prepend a random string to the domain name to avoid cache hits
	int length=6+rand()%7;	//random length between 6 and 12 characters
	for (int i=0; i<length; i++)
		name.append(charset, rand()%charset.length(), 1);  //random character

	name.append(".");
	name.append(domain);

	printf("Probing nameserver for domain %s with name %s\n",domain.c_str(),name.c_str());

	rname=ldns_dname_new_frm_str(name.c_str());

	s=ldns_resolver_new_frm_file(&resolver, NULL);	//use /etc/resolv.conf
	if (s!=LDNS_STATUS_OK) {
		printf("Error: could not find local resolver to probe for domain %s\n",domain.c_str());
		return -1;
	}

	//query and answer for rname A record
	//use recursion
	//gettimeofday(&start,NULL);
	p=ldns_resolver_query(resolver, rname, LDNS_RR_TYPE_A, LDNS_RR_CLASS_IN, LDNS_RD);
	//gettimeofday(&end,NULL);

	if (rname!=NULL) ldns_rdf_deep_free(rname);
	if (!p) {
		printf("Error: could not probe domain %s\n",domain.c_str());
		return -1;
	}

	//the query's rrt latency
	*latency=ldns_pkt_querytime(p);
	*timestamp=ldns_pkt_timestamp(p);

	printf("Latency for domain %s: %u milliseconds\n",domain.c_str(),*latency);
	//printf("my latency: %lu milliseconds\n",(end.tv_sec-start.tv_sec)*1000+(end.tv_usec-start.tv_usec)/1000);

	if (verbose) {
		//check and print response. In most cases it will be NXDOMAIN, with an empty rr_list
		a=ldns_pkt_rr_list_by_type(p, LDNS_RR_TYPE_A, LDNS_SECTION_ANSWER);
		if (!a) 
			printf("Name %s does not exist\n",name.c_str());	//expected due to the random string
		else {
			printf("Name %s exists\n",name.c_str());	//few domains have wildcard DNS records
			ldns_rr_list_sort(a); 
			ldns_rr_list_print(stdout, a);		//print the answer
			ldns_rr_list_deep_free(a);
		}
	}

	if (p!=NULL) ldns_pkt_free(p);
	if (resolver!=NULL) ldns_resolver_deep_free(resolver);

	return 0;
}

/*
 DB Schema:
 -Table timeseries
  domain | ts | latency

 -Table stats
  domain | avg_latency | std_latency | probes | ts_first | ts_last
*/

//if tables don't exist create them
void create_tables(mysqlpp::Connection& conn) {

	if (verbose) printf("creating DB tables if they don't exist\n");

	mysqlpp::Query query=conn.query();
	bool ret;

	query << "CREATE TABLE timeseries ( \
			domain VARCHAR(30) NOT NULL, \
			ts TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00', \
			latency INT(11) DEFAULT NULL)";
	ret=query.exec();	//ignoring errors, it will be due to existing table
	if (verbose) {
		if (ret==true) printf("table timeseries created\n");
		else printf("table timeseries existed\n");
	}

	query.reset();
	query << "CREATE TABLE stats ( \
			domain VARCHAR(30) NOT NULL, \
			avg_latency DOUBLE DEFAULT NULL, \
			std_latency DOUBLE DEFAULT NULL, \
			probes INT(11) DEFAULT NULL, \
			ts_first TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00', \
			ts_last TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00', \
			PRIMARY KEY (domain))";
	ret=query.exec();	//ignoring errors, ti will be due to existing table
	if (verbose) {
		if (ret==true) printf("table stats created\n");
		else printf("table stats existed\n");
	}
}

//initialize stats table for all domains that are not already initialized
void init_stats(mysqlpp::Connection& conn, int domainsNum) {

	if (verbose) printf("Initializing stats table\n");

	mysqlpp::Query query=conn.query();

	//insert one row for each domain in table stats
	//initialize all fields with zeros
	//if a row for this domain already exists, the insert fails (domain is primary key)
	//and data from previous runs remain in db, as we want
	for (int i=0; i<domainsNum; i++) {
		query.reset();
		query << "INSERT INTO stats VALUES (\"" << domains[i] << "\", 0, 0, 0, 0, 0);";
		bool ret=query.exec();
		if (verbose) {
			if (ret==true) printf("stats for domain %s initialized\n",domains[i].c_str());
			else printf("stats for domain %s exists in db\n",domains[i].c_str());
		}
	}
}

//update DB timeseries and stats for a domain
void updateDB(mysqlpp::Connection& conn, string domain, uint32_t latency, struct timeval timestamp) {

	if (verbose) printf("updating DB timeseries and stats for domain %s with latency %d\n",domain.c_str(),latency);

	mysqlpp::Query query=conn.query();

	//insert domain, timestamp, and latency to timeseries table
	query << "INSERT INTO timeseries VALUES (\"" << domain << "\", FROM_UNIXTIME(" << timestamp.tv_sec << "), " << latency << ");";
	query.exec();


	//update avg_latency, std_latency, probes, ts_first (if first probe) and ts_last to stats table for this domain
	query.reset();
	query << "SELECT * FROM stats WHERE domain=\"" << domain << "\"";
	mysqlpp::StoreQueryResult res=query.store();	//first get the previous stats for this domain
	if (!res) printf("Warning: cannot retrieve results for domain %s due to %s\n",domain.c_str(),query.error());
	else {
		//we expect a single row (domain is a primary key)
		double avg_latency=strtod(res[0][1].c_str(), NULL);	//prev average latency
		double std_latency=strtod(res[0][2].c_str(), NULL);	//prev std latency
		int probes=atoi(res[0][3].c_str());			//number of queries for this domain
		string ts_first_prev=res[0][4].c_str();

		if (verbose) printf("Previous stats for domain %s: avg_latency=%lf std_latency=%lf probes=%d\n",
				domain.c_str(),avg_latency,std_latency,probes);

		probes++;				//one more probe

		query.reset();
		query << "SELECT AVG(latency) FROM timeseries WHERE domain=\"" << domain << "\"";
		res=query.store();
		avg_latency=strtod(res[0][0].c_str(), NULL);  //update average latency for this domain

		query.reset();
		query << "SELECT STD(latency) FROM timeseries WHERE domain=\"" << domain << "\"";
		res=query.store();
		std_latency=strtod(res[0][0].c_str(), NULL);	//update std latency for this domain

		if (verbose) printf("Updating stats for domain %s: new avg_latency=%lf new std_latency=%lf probes=%d\n",
				domain.c_str(),avg_latency,std_latency,probes);

		stringstream ts_first;
		if (probes==1)		//in case it's the first probe, update ts_first
			ts_first << "FROM_UNIXTIME(" << timestamp.tv_sec << ")";
		else			//else keep the previous value
			ts_first << "\"" << ts_first_prev << "\"";

		query.reset();
		query << "UPDATE stats SET avg_latency=" << avg_latency << ", std_latency=" << std_latency << ", probes=" << probes \
			<< ", ts_first=" << ts_first.str() << ", ts_last=FROM_UNIXTIME(" << timestamp.tv_sec << ") WHERE domain=\"" << domain << "\"";
		query.exec();

	}
}

int main(int argc, char* argv[]) {
	int opt;

	//default parameters
	string filename="top-domains.csv";
	int topN=10;
	int interval=60;	//seconds
	int count=-1;		//probes
	string dbuser="papadog";
	string dbpass="papadog123";
	string dbserver="localhost";
	string dbname="dnsping";
	unsigned int dbport=0;
	int clearDB=0;
	int usingDB=0;

	//parse command line arguments
	while ((opt = getopt(argc, argv, "Chvf:n:i:c:u:p:s:d:P:"))!=EOF) {
		switch (opt) {
			case 'h':
				Usage(argv[0]);
				exit(1);
			case 'v':
				verbose=1;
				break;
			case 'C':
				clearDB=1;
				break;
			case 'f':
				filename=string(optarg);
				break;
			case 'n':
				topN=atoi(optarg);
				break;
			case 'i':
				interval=atoi(optarg);
				break;
			case 'c':
				count=atoi(optarg);
				break;
			case 'u':
				dbuser=string(optarg);
				break;
			case 'p':
				dbpass=string(optarg);
				break;
			case 's':
				dbserver=string(optarg);
				break;
			case 'd':
				dbname=string(optarg);
				break;
			case 'P':
				dbport=(unsigned int)atoi(optarg);
				break;
			default:
				Usage(argv[0]);
				exit(1);
		}
	}

	signal(SIGINT, terminate);
	signal(SIGQUIT, terminate);
	signal(SIGTERM, terminate);

	srand(time(NULL));

	//open file with top domains
	ifstream domainsFile;
	domainsFile.open(filename.c_str());
	if (!domainsFile) {
		printf("Error: file %s does not exist.\nPlease provide a valid file with domain names\n\n",filename.c_str());
		Usage(argv[0]);
		exit(1);
	}

	if (topN<=0) topN=10;
	if (interval<=0) interval=60;

	printf("Reading top %d domains from file: %s\n",topN,filename.c_str());
	printf("Probing each domain every %d seconds ",interval);
	if (count<0) printf("for ever\n");
	else printf("for %d times\n",count);

	if (verbose) printf("\nReading top %d domains:\n",topN);
	domains=new string[topN];

	//read topN domains from file 
	//it works with alexa's daily top 1M sites file 
	//http://s3.amazonaws.com/alexa-static/top-1m.csv.zip
	int domainsNum=0;
	string rank;
	while (!domainsFile.eof() && domainsNum<topN) {
		getline(domainsFile, rank, ',');
		getline(domainsFile, domains[domainsNum], '\n');
		if (verbose) printf("rank: %s name: %s\n",rank.c_str(),domains[domainsNum].c_str());
		domainsNum++;
	}
	domainsFile.close();

	if (domainsNum<topN)
		printf("Warning: found only %d domains (instead of %d)\n",domainsNum,topN);

	if (domainsNum==0) exit(1);

	//connect to DB
	mysqlpp::Connection conn((bool)false);
	if (!conn.connect(NULL, dbserver.c_str(), dbuser.c_str(), dbpass.c_str(), dbport)) {
		printf("Warning: database connection failed: %s\n",conn.error());
		printf("Warning: data will not be saved in database\n");
		usingDB=0;
	}
	else {
		usingDB=1;
		printf("\nConnected successfully to database\n");
		if (!conn.select_db(dbname)) {
			printf("Database %s does not exist\n",dbname.c_str());
			if (!conn.create_db(dbname)) {
				printf("Warning: database %s could not be created\n",dbname.c_str());
				printf("Warning: data will not be saved in database\n");
				usingDB=0;
			}
			else {
				conn.select_db(dbname);
				printf("Database %s created and selected\n",dbname.c_str());
			}
		}
		else printf("Database %s selected (existed)\n",dbname.c_str());

		if (clearDB==1) {	//drop tables to clear data
			printf("Cleaning database tables - previous data will be lost\n");
			mysqlpp::Query query=conn.query();
			query << "DROP TABLE timeseries";
			query.exec();
			query.reset();
			query << "DROP TABLE stats";
			query.exec();
		}

		//if tables don't exist create them
		//create one table for timeseries data (for all domains) 
		//(one table per each domain may be faster)
		//create one table for per-domain stats
		create_tables(conn);

		//initialize stats table for all domains that are not already initialized
		init_stats(conn, domainsNum);
	}


	//start probing nameservers
	int probe_counter=0;
	while (count==-1 || probe_counter<count) {

		printf("\nStarting probe %d\n",probe_counter+1);

		for (int domainsIter=0; domainsIter<domainsNum; domainsIter++) {
			uint32_t latency;
			struct timeval timestamp;
			int ret=probe(domains[domainsIter], &latency, &timestamp);	//probe a domain's nameserver
			if (ret!=-1 && usingDB==1)
				updateDB(conn, domains[domainsIter], latency, timestamp);  //update DB timeseries and stats for this domain
		}

		printf("Finished probe %d\n",probe_counter+1);
		probe_counter++;

		if (count==-1 || probe_counter<count) 
			sleep(interval);  //I should subtract the sum of the delays if accuracy was important
	}

	delete [] domains;
	domains=NULL;

	printf("Exiting\n");
	exit(1);
}


