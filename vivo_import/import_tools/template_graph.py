"""
Class for manipulating graph in VIVO.


.. moduleauthor:: rdj
"""
import ConfigParser
import csv
import getpass
from rdflib import (Graph, term)
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import sys
from vivo_sparql.sparql_query import SparqlQuery
from vivo_sparql.sparql_update import SparqlUpdate


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
        self._format = f

    def set_namespace(self, ns):
        self._namespace = ns

    def set_config(self, file_name, section=None):
        self.config = ConfigParser.ConfigParser()
        self.config.read(file_name)
        if section:
            self.set_config_section(section)

    def set_config_section(self, section):
        self.cfg_section = section

    def get_config(self, name):
        """
        Get config file entry from selected section of config file,
        if not specified, get from the main section.
        :param str name: Config entry to get
        """

        try:
            return self.config.get(self.cfg_section, name)
        except:
            return self.config.get("main", name)

    def __init__(self, config, section, fmt="nt"):
        """
        Initialize the template_graph, with specified config file and section.
        Load triples that are in the "added_triples" file, but not in "base_triples".
        """
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        Graph.__init__(self)
        self.config = ConfigParser.ConfigParser()
        self.config.read(config)
        if section:
            self.cfg_section = section
        self.set_format(fmt)
        self.set_namespace("http://vivo.dartmouth.edu/individual/")
        self._umap = {}
        self._all = True
        self._filter = False

        fn = self.get_config("added_triples")
        self.parse(fn, format=self._format)

        try:
            fn = self.get_config("base_triples")
            if fn:
                self.subtract(fn)
                print "new triples:"
                print super(TemplateGraph, self).serialize(format=self._format)
        except:
            pass

    def _uri(self, uri, rand=True):
        """
        Get URI for the item we are inserting
        """
        try:
            uri = str(uri)
        except:
            pass

        try:
            return self._uri_map[uri]
        except:
            pass

        if uri == self._added_uri:
            self._uri_map[uri] = term.URIRef(self._update_namespace(
                self._namespace + self._uri_prefix + self._row[self._uidi]))

        else:
            if rand and uri.startswith(self._namespace + "n"):
                if self._next_id:
                    new_id = self._next_id
                    self._next_id += 1
                else:
                    new_id = self.sparql.get_available_id()

                self._uri_map[uri] = term.URIRef(self._update_namespace(
                    "%s%s%s" % (self._namespace, "n", new_id)))

            else:
                return term.URIRef(self._update_namespace(uri))

        return self._uri_map[uri]

    def _update_namespace(self, u):
        if not self._added_namespace:
            return u

        return u.replace(self._added_namespace, self._destination_namespace)

    def _fill_in(self, item):
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
            rand = True
            for w in s:
                if uri:
                    v = int(w[:3])
                    uri += self._row[v]
                    uri += w[3:]
                    rand = False
                else:
                    uri += w
            u = self._uri(uri, rand)
            return u

        elif isinstance(item, term.Literal):
            val = ''

            s = item.split('$VAR')
            val = s[0]
            for w in s[1:]:
                v = int(w[:3])
                val += self._row[v]
                val += w[3:]
            val = self._update_namespace(val)

            return term.Literal(val, datatype=item.datatype)

        return item

    def has_content(self, s):
        """
        Check for non white-space content in a string.
        :param str s: String to check for content
        :returns The string with white-space tags removed
        """
        blanks = ['<p>', '</p>', '<br>', '&nbsp;']
        s = str(s)
        for b in blanks:
            s = s.replace(b, '')
        s = s.strip()
        return s

    def _getq(self, s):
        """
        Translate a subject or object - update with destination namespace,
        and change variables to ?v### format.
        :param str s: Subject or Object to translate
        :returns: Translated version.
        """
        gq = s
        if str(s) == str(self._added_uri):
            return '?v%03d' % self._uidi


        if s.startswith(self._namespace + 'n'):
            return '?' + s[len(self._namespace):]

        if '$VAR' not in s:
            return s
        s = s.split('$VAR')
        v = int(s[1][:3])

        if str(s[0]).startswith(self._added_namespace):
            s[0]=s[0].replace(self._added_namespace, self._destination_namespace)

        self.vmap[v] = s
        return "?v%03d" % v


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
                elif self._import:
                    if self._used[item[0]] and ((item[2] not in self._used) or self._used[item[2]]):
                        subj = self._fill_in(item[0])
                        obj = self._fill_in(item[2])
                        if str(obj)==str(item[2]):
                            obj = item[2]
                        y = (subj, item[1], obj)

                        if self.has_content(obj):
                            yield y
                else:
                    obj = self._getq(item[2])
                    if obj==item[2] and isinstance(item[2], term.Literal):
                        obj = item[2].n3()
                    y = (self._getq(item[0]), self._getq(item[1]), obj)
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
                        self.remove(trip)
                        trip = (self._replace(trip[0], col, n), trip[1],
                                self._replace(trip[2], col, n))
                        self._imap[n].add(trip[0])

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
        """
        Create the map of which csv columns map to which triples, and which triples reference each other.
        """
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

    def insert_row(self, row):
        """
        Insert a row of data into VIVO
        :param row: List of column data for this row.
        """
        self._uri_map = {}
        self._init_used()
        self._mark_input_used(row)
        self._use_connections()
        self._row = row

        ser = self.serialize()

        self.sparqlUpdate.insert(ser)

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

    def _read_config(self):
        self._csv_file = self.get_config("csv_file")
        self._added_uri = self.get_config("added_uri")
        self._uri_prefix = self.get_config("uri_prefix")
        self._destination_namespace = self.get_config("destination_namespace")
        if self._destination_namespace:
            self._added_namespace = self.get_config("added_namespace")
        else:
            self._added_namespace = None
        self._uri_id = self.get_config("uri_id_column")
        self._vivo_url = self.get_config("vivo_url")

        self._sparql_endpoint = self._vivo_url + "api/sparqlUpdate"
        try:
            outfile = self.get_config("output_file")
            if outfile:
                self._sparql_endpoint = outfile
        except:
            pass

        self._vivo_account = self.get_config("vivo_account")
        self._vivo_pwd = self.get_config("vivo_pwd")

        if not self._vivo_pwd:
            self._vivo_pwd = getpass.getpass("VIVO Password: ")
        self._next_id = 0

    def _init_sparql(self):
        self.sparql = SparqlQuery(self._vivo_url, self._vivo_account,
                                  self._vivo_pwd)
        self.sparql.set_namespace(self._destination_namespace)

    def _get_key(self):
        """
        Read the .key file and parse it's mapping information.

        """
        with open(self._csv_file, "rU") as f:
            self._dialect = csv.Sniffer().sniff(f.read(10240))
            self._dialect.doublequote = True

        with open(self._csv_file+".key", "rU") as f:
            reader = csv.reader(f, self._dialect)
            mapping = 0
            self._uidi = -1

            header = reader.next()
            mapping = reader.next()
            self.set_map(mapping)
            self._uidi = -1

            # Look for uri id column in header (first row of csv file)
            for i, m in enumerate(header):
                if m == self._uri_id:
                    self._uidi = i

    def show_trips(self, trips, subs, depth):
        q = ''

        for trip in trips:
            if trip[3]:
                q += ' '*depth
                q += 'optional { %s' % trip[2]
                try:
                    if trip[1][1] < 0:
                        t = subs[trip[1][1]]
                        del subs[trip[1][1]]
                        q += '\n'
                        q += self.show_trips(t, subs, depth + 1)
                except:
                    pass
                q += ' '*depth
                q += '}\n'
            else:
                q += ' '*depth
                q += trip[2] + '\n'
        return q

    def _fill_trip(self, t, data):
        if t.startswith('?n'):
            try:
                v = int(t[2:])
                return data[v]
            except:
                return "?ERROR %s" % t

        return self._fill_in(t)


#    def _replace_uri(self, s, data):
#        if str(s) == self._added_uri:
#            return data[0]
#
#        if s.startswith(self._namespace + 'n'):
#            try:
#                v = int(s[len(self._namespace)+1:])
#                return data[v]
#            except:
#                pass
#        return s


#    def get_var_trip(self, val, v, data):
#        rval = None

#        self._import = True
#        self._filter = False
#        self._row = {v: val}
#        for i in self:
#            try:
#                o = self._fill_in(i[2]).n3()
#                if str(o[1:-1]) != str(i[2]):
#                    s = self._replace_uri(i[0], data)
#                    rval = ('<%s>' % s, i[1].n3(), o)
#            except:
#                pass
#
#        return rval

    def get_vivo_data(self, link_map = True):
        """
        Get row data from VIVO.
        :param Boolean link_map: Indicate whether or not to include map of sub uri's.
        :returns row data
        """
        self._import = False
        self._read_config()
        self._init_sparql()
        self._get_key()
        lines = []
        for i in self:
            lines.append(i)
        self._filter = True
        vs = set([])

        q = ""
        trips = []
        self.vmap = {}
        for i in self:
            trip = ""
            vcnt = 0
            req = False
            vv = []

            for n in i:
                if n[0]=='?':
                    trip += "%s " % n
                    if n[1]=='v':
                        vs.add(int(n[2:]))
                        vv.append(int(n[2:]))
                    else:
                        vs.add(-int(n[2:]))
                        vv.append(-int(n[2:]))

                    vcnt += 1
                    if int(n[2:])==self._uidi:
                        req = True
                else:
                    nn = n
                    try:
                        nn = n.n3()
                    except:
                        pass

                    trip += "%s " % nn
            trip += ".\n"
            vvv = 9999999
            if vv[0] < 0:
                vvv = -vv[0]
            if req and vcnt==1:
                vvv = 0
            trips.append((vvv, vv, trip.strip(), vcnt!=1))

        trips = sorted(trips)
        skip = 0
        req = 0
        subs = {}

        for n,trip in enumerate(trips):
            if trip[0]==0:
                req += 1
                skip += 1
            elif trip[1][0]<0:
                if trip[1][0] not in subs:
                    subs[trip[1][0]] = [trip]
                else:
                    subs[trip[1][0]].append(trip)
                skip += 1

        trips = trips[:req] + trips[skip:]

        subu = set()
        for i in self:
            if i[0].startswith('?n'):
                subu.add(i[0])

        q = "Select "
        for var in sorted(vs):
            if var>=0:
                q += '?v%03d ' % var
        for var in sorted(subu):
            q += '%s ' % var

        q += ' where {\n'
        q += self.show_trips(trips, subs, 1)

        q += '}'

        g = self.sparql.doQuery(q)
        self.vmap = {}
        for i in self:
            pass


        if self._uidi >= 0:
            self.vmap[self._uidi] = ["%s/%s" % (self._destination_namespace, self._uri_prefix), "%03d"%self._uidi]

        columns = 0
        for row in g:
            if len(row) > columns:
                columns = len(row)

        rval = []
        for row in g:
            umap = {}
            outrow = []
            bad = False
            for col in sorted(row):
                cnum = int(col[1:])
                if col[0]=='n':
                    umap[cnum] = str(row[col]['value'])
                else:
                    while len(outrow) <= columns:
                        outrow.append('')
                    s = str(row[col]['value'])
                    try:
                        if self.vmap[cnum][0]:
                            s = s.split(self.vmap[cnum][0])[1]
                        outrow[cnum] = s
                    except:
                        bad = True
            if not bad:
                try:
                    if link_map:
                        umap[0] = "%s/%s%s" % (self._destination_namespace, self._uri_prefix, outrow[0])
                        outrow[-1] = umap
                except:
                    pass
                rval.append(outrow)

        return rval

    def _export(self, fn):
        rows = self.get_vivo_data(False)
        with open(fn, 'w') as of:
            writer = csv.writer(of, lineterminator='\n')
            for row in rows:
                writer.writerow(row)
        return len(rows)


    def _import(self, rows=None):
        self._import = True
        self._read_config()

        self.sparqlUpdate = SparqlUpdate(self._sparql_endpoint,
                                         self._vivo_account, self._vivo_pwd)
        self._init_sparql()

        self._get_key()

        if rows:
            for row in rows:
                self.insert_row(row)
        else:
            with open(self._csv_file, "rU") as f:
                reader = csv.reader(f, self._dialect)
                header = reader.next()  # ignore header
                for row in reader:
                    self.insert_row(row)

    def import_csv(self):
        self._import()
        self.sparqlUpdate.close()

    def import_rows(self, rows):
        self._import(rows)
        self.sparqlUpdate.close()

    def export_csv(self, fn='export.csv'):
        return self._export(fn)

    def move_uris(self, uris):
        """
        Update triples with uri changes provided.  Update both Subjects and Objects.
        :param uris: List of (old, new) pairs, where old URI will be updated with new.

        """
        self._read_config()
        self.sparqlUpdate = SparqlUpdate(self._sparql_endpoint,
                                         self._vivo_account, self._vivo_pwd)
        self._init_sparql()
        for move in uris:
            self.sparqlUpdate.move_uri(move[0], move[1])
