"""
Tool for importing csv data into VIVO.
Usage: import_csv.py <config section>

.. moduleauthor:: rdj
"""
import ConfigParser
import sys
from template_graph import TemplateGraph
from vdm.pubmed import import_pubmed


def add_authorship(uid, pmid):
    """
    Import an article from pubmed into VIVO, and connect to the author.
    :param str uid: ID of article's author
    :param str pmid: PubMed ID of article
    """
    article = import_pubmed(pmid)

    g = TemplateGraph("vivo_csv.cfg", "articles")

    article = [article['pubmed'], article['pmc'], article['doi'],
               article['mid'], article['title'], article['year'],
               article['authors'], article['journal'], article['issue'],
               article['volume'], None, article['end_page']]
    g.import_rows([article])

    g = TemplateGraph("vivo_csv.cfg", "authors")
    rows = [[uid, str(pmid)]]
    g.import_rows(rows)


def exclude_authorship(uid, pmid):
    """
    Add article to an author's "exclude" list in VIVO.
    :param str uid: ID of Author to exclude article
    :param str pmid: PubMed ID of article
    """

    g = TemplateGraph("vivo_csv.cfg", "exclude")
    g.import_rows([[uid, str(pmid)]])


def main():
    config = ConfigParser.ConfigParser()
    config.read("vivo_csv.cfg")
    cfg_section = sys.argv[1]

    g = TemplateGraph("vivo_csv.cfg", cfg_section)
    g.import_csv()


if __name__ == "__main__":
    main()
