"""
Application to fill in missing Journal information for publications in VIVO.

.. moduleauthor:: rdj
"""
import ConfigParser
import sys
import re
from template_graph import TemplateGraph
from vivo_sparql.sparql_query import SparqlQuery
from vivo_sparql.sparql_update import SparqlUpdate
from vdm.pubmed import import_pubmed

def clean(val):
    return val.replace('&amp;', '&')


def main():
    next_journal = 1845
    config = ConfigParser.ConfigParser()
    config.read("vivo_csv.cfg")
    vivo = config.get("main", "vivo_url")
    account = config.get("main", "vivo_account")
    pwd = config.get("main", "vivo_pwd")

    sparql = SparqlQuery(vivo, account, pwd)
    sparqlUpdate = SparqlUpdate(vivo + "api/sparqlUpdate", account, pwd)

    # Get list of Journals currently in VIVO
    journals = sparql.get_journals()
    jname = {}
    jnum = {}

    for journal in journals:
        jid = int(re.search(r'\d+$', journal[1]).group())
        jname[jid] = journal[0]
        jnum[journal[0]] = jid

    # Get list of VIVO articles that have no journal information.
    arts = sparql.get_journalless_articles()
    for art in arts:
        # Look up article's journal information on PubMed
        pubmed = import_pubmed(art[1])
        j = clean(pubmed['journal'])

        print art[1], j

        if j in jnum: # Journal is already in VIVO
            print "jnum:", jnum[j]
        else: # Journal not in VIVO - add it
            print "New Journal"
            while next_journal in jname:
                next_journal += 1
            print "ID: %d, Name: %s" % (next_journal, j)
            g = TemplateGraph("vivo_csv.cfg", "journals")
            rows = [[str(next_journal), j]]
            g.import_rows(rows)
            jname[next_journal] = j
            jnum[j] = next_journal

        # Connect the article to the journal
        g = TemplateGraph("vivo_csv.cfg", "pubs")
        rows = [[art[1], None, None, None, None, None, None, str(jnum[j]), None, None, None, None]]
        print "Rows:"
        print rows
        g.import_rows(rows)




if __name__ == "__main__":
    main()
