/* 
 * dbquery - A simple program to query the DB for dnsping results
 * Antonis Papadogiannakis <papadog@ics.forth.gr>
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
#include <mysql++.h>

using namespace std;

string *domains=NULL;

void Usage(char *pname) {
	printf("Usage: %s ",pname);
	printf("[-f filename] [-n number] [-D domain]\n");
	printf("\t[-u DBuser] [-p DBpassword] [-s DBserver] [-P DBport] [-d DBname] [th]\n\n");

	printf(" [-f filename] \tfilename with top domains to query (default: none)\n");
	printf(" [-n number] \tnumber of top domains to query from file (default: 10)\n");
	printf(" [-D domain] \tretrieve results for a specific domain\n");
	printf(" [-u DB user] \tusername for mysql database (default: papadog)\n");
	printf(" [-p DBpasswd] \tpassword for mysql database (default: ******)\n");
	printf(" [-s DBserver] \tmysql server (default: localhost)\n");
	printf(" [-P DBport] \tmysql server port (default: none)\n");
	printf(" [-d DBname] \tdatabase name (default: dnsping)\n");
	printf(" [-t] \t\tPrint full timeseries also\n");
	printf(" [-h] \t\thelp\n");
}

//handle signals
void terminate(int signo) {

	if (domains!=NULL) 
		delete [] domains;
	domains=NULL;
	printf("Stopped\n");
	exit(1);
}

int main(int argc, char* argv[]) {
	int opt;

	//default parameters
	string filename;
	int topN=10;
	int domainsNum=0;
	string dbuser="papadog";
	string dbpass="papadog123";
	string dbserver="localhost";
	string dbname="dnsping";
	unsigned int dbport=0;
	int printTimeseries=0;
	string domain;

	//parse command line arguments
	while ((opt = getopt(argc, argv, "thf:n:D:u:p:s:d:P:"))!=EOF) {
		switch (opt) {
			case 'h':
				Usage(argv[0]);
				exit(1);
			case 't':
				printTimeseries=1;
				break;
			case 'f':
				filename=string(optarg);
				break;
			case 'n':
				topN=atoi(optarg);
				break;
			case 'D':
				domain=string(optarg);
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

	if (filename.empty() && domain.empty())
		printf("Quering for all domains in database\n");

	else if (!filename.empty()) {
		//open file with top domains
		ifstream domainsFile;
		domainsFile.open(filename.c_str());
		if (!domainsFile) {
			printf("Error: file %s does not exist.\nPlease provide a valid file with domain names\n\n",filename.c_str());
			Usage(argv[0]);
			exit(1);
		}

		if (topN<=0) topN=10;
		printf("Reading top %d domains from file: %s\n",topN,filename.c_str());
		domains=new string[topN];

		domainsNum=0;
		string rank;
		while (!domainsFile.eof() && domainsNum<topN) {
			getline(domainsFile, rank, ',');
			getline(domainsFile, domains[domainsNum], '\n');
			domainsNum++;
		}
		domainsFile.close();

		if (domainsNum<topN)
			printf("Warning: found only %d domains (instead of %d)\n",domainsNum,topN);

		if (domainsNum==0) exit(1);
	}

	else if (!domain.empty())
		printf("Quering for domain: %s\n",domain.c_str());

	//connect to DB
	mysqlpp::Connection conn((bool)false);
	if (!conn.connect(dbname.c_str(), dbserver.c_str(), dbuser.c_str(), dbpass.c_str(), dbport)) {
		printf("Database connection failed: %s\n",conn.error());
		exit(1);
	}

	printf("\nConnected successfully to database\n");

	mysqlpp::Query query=conn.query();

	if (printTimeseries==1) {
		printf("\nFull Timeseries:\n");
		printf("| domain\t| ts\t\t\t| latency |\n");

		query.reset();
		if (filename.empty() && domain.empty())
			query << "SELECT * FROM timeseries";
		else if (!filename.empty()) {
			query << "SELECT * FROM timeseries WHERE domain=\"" << domains[0] << "\"";
			for (int i=1; i<domainsNum; i++)
				query << " OR domain=\"" << domains[i] << "\"";
		}
		else if (!domain.empty())
			query << "SELECT * FROM timeseries WHERE domain=\"" << domain << "\"";

		mysqlpp::StoreQueryResult res=query.store();
		mysqlpp::StoreQueryResult::const_iterator it;

		for (it=res.begin(); it!=res.end(); ++it) {
			mysqlpp::Row row = *it;
			cout << "| " << row[0] << "\t| " << row[1] << "\t| " << row[2] << "\t|" << endl;
		}
	}

	printf("\nAggregate stats per-domain:\n");
	printf("| domain\t| avg_latency\t| std_latency\t| probes| ts_first\t\t| ts_last\t\t|\n");

	query.reset();
	if (filename.empty() && domain.empty())
		query << "SELECT * FROM stats";
	else if (!filename.empty()) {
		query << "SELECT * FROM stats WHERE domain=\"" << domains[0] << "\"";
		for (int i=1; i<domainsNum; i++)
			query << " OR domain=\"" << domains[i] << "\"";
	}
	else if (!domain.empty())
		query << "SELECT * FROM stats WHERE domain=\"" << domain << "\"";

	mysqlpp::StoreQueryResult res=query.store();
	mysqlpp::StoreQueryResult::const_iterator it;

	for (it=res.begin(); it!=res.end(); ++it) {
		mysqlpp::Row row = *it;
		cout << "| " << row[0] << "\t| " << row[1] << "\t\t| " << row[2] << "\t\t| " << row[3] << "\t| " \
			<< row[4] << "\t| " << row[5] << "\t|" << endl;
	}


	if (domains!=NULL) 
		delete [] domains;
	domains=NULL;

	return 1;
}


