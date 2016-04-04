# -*- coding: utf-8 -*-
"""
Class for accessing VIVO Sparql Update API to Insert or Delete data

.. moduleauthor:: rdj
"""
from SPARQLWrapper import SPARQLWrapper
import getpass
import sys
import traceback


class SparqlUpdate(SPARQLWrapper):
    def set_graph(self, graph):
        self._graph = graph


    def delete(self, query):
        """
        Delete data from VIVO.
        :param str query: The query string to be deleted.
        """
        qstr = "DELETE DATA {GRAPH <%s> {%s}}" % (self._graph, query)
        qstr = "DELETE WHERE {GRAPH <%s> {%s}}" % (self._graph, query)
        self.setQuery(qstr)

        try:
            results = self.query().convert()
            if "update accepted" not in results:
                print "results:", results
        except:
            print "Delete failed"
            traceback.print_exc(file=sys.stdout)


    def insert(self, query):
        """
        Insert data into VIVO.
        :param str query: The query string to be inserted.
        """
        self.__count += 1
        if self.__tfile:
            self.__tfile.write(query)
            return

        qstr = "INSERT DATA {GRAPH <%s> {%s}}" % (self._graph, query)
        self.setQuery(qstr)

        try:
            results = self.query().convert()
            if "update accepted" not in results:
                print "results:", results
        except:
            print "Insert failed"
            traceback.print_exc(file=sys.stdout)

    def __init__(self, endpoint, account, pwd):
        self.__count = 0
        self.__tfile = None

        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            SPARQLWrapper.__init__(self, endpoint)
            if not pwd:
                pwd = getpass.getpass()

            self.addParameter('email', account)
            self.addParameter('password', pwd)
            self.method = 'POST'
            self.set_graph(
                "http://vitro.mannlib.cornell.edu/default/vitro-kb-2")


        else:
            self.__tfile = open(endpoint, "w")

    def close(self):
        if self.__tfile:
            self.__tfile.close()
