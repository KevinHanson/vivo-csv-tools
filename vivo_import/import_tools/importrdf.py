"""
Tool for importing large RDF files into VIVO.

.. moduleauthor:: rdj
"""
import ConfigParser
import sys
from vivo_sparql.sparql_query import SparqlQuery
from vivo_sparql.sparql_update import SparqlUpdate

CHUNK = 100

def main():
    config = ConfigParser.ConfigParser()
    config.read("vivo_csv.cfg")
    vivo = config.get("main", "vivo_url")
    account = config.get("main", "vivo_account")
    pwd = config.get("main", "vivo_pwd")

#    sparql = SparqlQuery(vivo, account, pwd)
    sparqlUpdate = SparqlUpdate(vivo + "api/sparqlUpdate", account, pwd)

    with file(sys.argv[1]) as f:
        lines = ""
        cnt = 0
        pcnt = 0
        for line in f:
            lines += line
            cnt += 1
            if cnt >= CHUNK:
                cnt=0;
                pcnt += 1
                sparqlUpdate.insert(lines)
                print "--- %d ---" % pcnt
                lines = ""
        if cnt > 0:
            print "--- last ---"
            sparqlUpdate.insert(lines)


if __name__ == "__main__":
    main()
