from SPARQLWrapper import SPARQLWrapper
import getpass
import sys
import traceback

class Sparql(SPARQLWrapper):
    def set_graph(self, graph):
        self._graph = graph

    def insert(self, query):
        print "INSERT:"
        print query
        self.__count += 1
        print "Count:", self.__count
        if self.__tfile:
            self.__tfile.write(query)
            return

        self.setQuery("INSERT DATA {GRAPH <%s> {%s}}" % (self._graph, query))

        try:
            self.query()
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
            self.set_graph("http://vitro.mannlib.cornell.edu/default/vitro-kb-2")
        else:
            self.__tfile = open(endpoint, "a")
