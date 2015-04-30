from rdflib import (Graph, term)
import csv
from sparql import Sparql


class TemplateGraph(Graph):

    def subtract(self, fn):
        """
        Read a graph from a file, and subtract it from this graph.
        :param fn: Path to the graph.
        """
        g = Graph()
        g.parse(fn, format=self._format)
        self.__isub__(g)

    def set_format(self, f):
        """
        Set file format for input and output.
        :param fmt: Text indicating the format.  See Graph for list of options.
        """
        self._format = f

    def set_namespace(self, ns):
        self._namespace = ns

    def __init__(self, fn, fmt="nt"):
        Graph.__init__(self)
        self.set_format(fmt)
        self.parse(fn, format=self._format)
        self.set_namespace("http://vivo.dartmouth.edu/individual/")
        self._umap = {}
        self._all = True
        self._filter = False

    def _uri(self, uri):
        try:
            return self._uri_map[uri]
        except:
            pass

        if str(uri) == self._added_uri:
            self._uri_map[uri] = term.URIRef(self._update_namespace(
                self._namespace + self._uri_prefix + self._row[self._uidi]))

        else:
            if uri.startswith(self._namespace + "n"):
                self._uri_map[uri] = term.URIRef(self._update_namespace(
                    "%s%s%s" % (self._namespace, "nn", self._next_id)))
                self._next_id += 1
            else:
                return term.URIRef(self._update_namespace(uri))

        return self._uri_map[uri]

    def _update_namespace(self, u):
        if not self._added_namespace:
            return u

        return u.replace(self._added_namespace, self._destination_namespace)

    def _process(self, item):
        """
        Replace $VAR### in the given item with values from self._row[###]
        :param item: Subject or Object of a triple.  Can be either
        term.URIRef or term.Literal.
        :returns The Subject or Object with row data in place of
        and $VAR###'s found.
        """

        if "$VAR" not in item:
            return self._uri(item)

        if isinstance(item, term.URIRef):
            s = item.split("$VAR")
            uri = ""
            for w in s:
                if len(uri):
                    v = int(w[:3])
                    uri += self._row[v]
                    uri += w[4:]
                else:
                    uri += w
            u = self._uri(uri)
            return u

        elif isinstance(item, term.Literal):
            val = ""

            s = item.split("$VAR")
            val = s[0]
            for w in s[1:]:
                v = int(w[:3])
                val += self._row[v]
                val += w[3:]
            val = self._update_namespace(val)
            return term.Literal(val, datatype=item.datatype)

        return item

    def __iter__(self):
        """
        If self._filter is True, only URI's that are True in _used will
        be iterated, and they will have $VAR###'s replace with row data.
        Otherwise, the triples are iterated as normal.
        """
        for item in self.triples((None, None, None)):
            try:
                if (not self._filter):
                    yield item
                elif self._used[item[0]]:
                    y = (self._process(item[0]),
                         item[1], self._process(item[2]))
                    yield y
                elif not self._used[item[0]]:
                    y = (self._process(item[0]),
                         item[1], self._process(item[2]))
                    yield y
            except:
                pass

    def _replace(self, t, text, n):
        """
        Replace the substring given in text with $VAR###
        :param t: Subject or Object of triple to have the replacement done.
        :param text: The text to replace
        :param n: Integer value to put in the ### part of $VAR###
        :returns The given subject or object with the replacement done.
        """
        if isinstance(t, term.URIRef):
            return term.URIRef(t.replace(text, "$VAR%03d" % n))
        elif isinstance(t, term.Literal):
            return term.Literal(t.replace(text, "$VAR%03d" % n),
                                datatype=t.datatype)
        return t

    def _map_input(self, row):
        """
        Go through all triples, and replace the strings in row with
        $VAR### where for the text in row[###].  Also create a map
        of all URI's that take input from each column.

        :param row: Column data to look for.  Blank columns are ignored.
        This should be the sample data that was manually entered into VIVO.
        """
        for trip in self:
            for n, col in enumerate(row):
                if col:
                    if (col in trip[0]) or (col in trip[2]):
                        self._imap[n].add(trip[0])
                        self.remove(trip)
                        trip = (self._replace(trip[0], col, n), trip[1],
                                self._replace(trip[2], col, n))
                        self.add(trip)

    def _map_links(self):
        """
        Makes a list of connections between triples' subject and object values,
        when both are URIRef's.
        """
        for trip in self:
            if isinstance(trip[2], term.URIRef):
                if trip[2].startswith(self._namespace):
                    try:
                        self._umap[trip[0]].add(trip[2])
                    except:
                        self._umap[trip[0]] = set([trip[2]])

    def _init_used(self):
        """
        Create an initial map of all URI's that might be added for a row of
        data, with all of them set to False (unused).
        """
        for m in self._umap:
            self._used[m] = False
            for i in self._umap[m]:
                self._used[i] = False

        for m in self._imap:
            for i in m:
                self._used[i] = False

    def _mark_input_used(self, row):
        """
        Mark as used all URI's that want data that was supplied for this row.

        :param row: List of column data for one line of data.
        """
        for n, item in enumerate(row):
            if item:
                try:
                    for t in self._imap[n]:
                        self._used[t] = True
                except:
                    pass

    def set_map(self, row):
        self._imap = []
        self._used = {}
        for n in row:
            self._imap.append(set())

        self._map_input(row)
        self._map_links()
        self._mapping = row

    def _walk(self, uri):
        """
        Recursively follow all connections until a used URI is found, or
        all connection paths are exhausted.  If a used URI is found, mark
        all URI's on the path as used.
        :param connections: All URI's connected as objects from this URI
        """
        if uri in self._visited:
            return False

        if self._used[uri]:
            return True

        self._visited.append(uri)

        try:
            for point in self._umap[uri]:
                if self._walk(point):
                    self._used[uri] = True
        except:
            pass
        self._visited.pop()
        return self._used[uri]

    def _use_connections(self):
        """
        Mark URIs that are needed to connect the supplied data.
        This will leave out URIs that don't have any data in them,
        and do not connect two or more URIs that have data.

        Starting from any used URI, find all paths through URIs'
        subject-->object connections to other used URIs, and mark those paths
        as used.
        """
        for u in self._umap:
            if self._used[u]:
                self._visited = [u]
                for c in self._umap[u]:
                    self._walk(c)

    def process_row(self, row):
        """
        Print out the triples for a row of data.
        :param row: List of column data for this row.
        """
        self._uri_map = {}
        self._init_used()
        self._mark_input_used(row)
#        self._remove_unused()
        self._use_connections()
        self._row = row
        self.sparql.insert(self.serialize())

    def serialize(self):
        """
        Serializes only URI's that are used for this row of data, with
        the data put in place.
        :returns The serialization.
        """
        self._filter = True
        s = super(TemplateGraph, self).serialize(format=self._format)
        self._filter = False
        return s

    def import_csv(self, csv_file, added_uri, uri_prefix, added_namespace,
                   destination_namespace, uri_id, start_id, sparql_endpoint,
                   vivo_account, vivo_pwd):
        self._added_uri = added_uri
        self._uri_prefix = uri_prefix
        self._next_id = start_id
        if destination_namespace:
            self._added_namespace = added_namespace
            self._destination_namespace = destination_namespace
        else:
            self._added_namespace = None

        print "csv file:", csv_file
        self.sparql = Sparql(sparql_endpoint, vivo_account, vivo_pwd)

        with open(csv_file, "rU") as f:
            dialect = csv.Sniffer().sniff(f.read(1024))
            dialect.doublequote = True

        with open(csv_file+".key", "rU") as f:
            reader = csv.reader(f, dialect)
            mapping = 0
            self._uidi = -1

            reader.next()
            mapping = reader.next()
            self.set_map(mapping)
            print "Key:"
            print super(TemplateGraph, self).serialize(format="nt")

        with open(csv_file, "rU") as f:
            reader = csv.reader(f, dialect)
            header = 0
            self._uidi = -1

            # Look for uri id column in header (first row of csv file)
            header = reader.next()
            for i, m in enumerate(header):
                if m == uri_id:
                    self._uidi = i

            for row in reader:
                self.process_row(row)
