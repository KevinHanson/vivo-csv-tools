# -*- coding: utf-8 -*-
"""
Class for accessing VIVO Sparql Query API

.. moduleauthor:: rdj
"""
from SPARQLWrapper import SPARQLWrapper, JSON
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed
from SPARQLWrapper.SPARQLExceptions import EndPointNotFound
from SPARQLWrapper.SPARQLExceptions import EndPointInternalError
from random import randrange
import getpass
import requests
import sys
import traceback
import urllib
import urllib2

class SparqlQuery(SPARQLWrapper):
    def set_graph(self, graph):
        self._graph = graph

    def set_namespace(self, ns):
        self._namespace = ns

    def get_person_text(self, uid):
        """
        Dartmouth-specific function to get text from a faculty member's
        VIVO profile.
        """
        words = ""

        query = """
SELECT ?overview ?researchO ?label
WHERE
{
      <%s> <http://vivoweb.org/ontology/core#overview> ?overview .
      <%s> <http://vivoweb.org/ontology/core#researchOverview> ?researchO .
      <%s> <http://www.w3.org/2000/01/rdf-schema#label> ?label .
}
        """ % (uid, uid, uid)
        self.setQuery(query)
        try:
            rval = self.query()
            try:
                g = rval.convert()
            except:
                pass
            words = "%s %s %s" % (g['results']['bindings'][0]['overview']['value'], g['results']['bindings'][0]['researchO']['value'], g['results']['bindings'][0]['label']['value'])
        except:
            print "Select failed: %s" % query

        self.setQuery("""
PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:     <http://www.w3.org/2000/01/rdf-schema#>
PREFIX vivo:     <http://vivoweb.org/ontology/core#>
PREFIX xsd:      <http://www.w3.org/2001/XMLSchema#>
SELECT ?name
WHERE
{
  ?auth vivo:relates <%s> .
  ?auth rdf:type vivo:Authorship .
  ?auth vivo:relates ?art .
  filter (?art!=<%s>) .
  ?art <http://vivoweb.org/ontology/core#dateTimeValue> ?date .
  ?date <http://vivoweb.org/ontology/core#dateTime> ?year .
  filter (?year>"2009-01-01T00:00:00Z"^^xsd:dateTime) .
  ?art rdfs:label ?name .
}
LIMIT 20
""" % (uid, uid))
        try:
            rval = self.query()
            try:
                g = rval.convert()
            except:
                pass
            for t in g['results']['bindings']:
                words = words + " " + t['name']['value']

        except:
            print "Select failed"
            traceback.print_exc(file=sys.stdout)

        self.setQuery("""
PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:     <http://www.w3.org/2000/01/rdf-schema#>
PREFIX vivo:     <http://vivoweb.org/ontology/core#>
PREFIX xsd:      <http://www.w3.org/2001/XMLSchema#>

SELECT ?name
WHERE
{
  ?grant vivo:relates <%s> .
  ?grant rdf:type vivo:Grant .
  ?grant <http://vivoweb.org/ontology/core#dateTimeInterval> ?date .
  ?date <http://vivoweb.org/ontology/core#end> ?end .
  ?end <http://vivoweb.org/ontology/core#dateTime> ?year .
  filter (?year>"2009-01-01T00:00:00Z"^^xsd:dateTime) .
  ?grant rdfs:label ?name .
}

        """ % (uid))
        try:
            rval = self.query()
            try:
                g = rval.convert()
            except:
                pass

            for t in g['results']['bindings']:
                words = words + " " + t['name']['value']

        except:
            print "Select failed"
            traceback.print_exc(file=sys.stdout)




        return words

    def lookup_netid(self, netid):
        """
        Dartmouth-specific function to look up URI for faculty
        member with a given netid.
        """
        self.setQuery("""Select ?uid where {
           ?who <http://vivo.dartmouth.edu/ontology/netId> "%s" .
           ?who <http://vivo.dartmouth.edu/ontology/geiselId> ?uid .
        }""" % (netid))

        try:
            rval = self.query()
            try:
                g = rval.convert()
            except:
                pass
            return g['results']['bindings'][0]['uid']['value']
        except:
            return None

    def get_journals(self):
        """
        Get list of journals in VIVO.
        :returns The list of journals: (name, uri)
        """
        self.setQuery("""Select ?what ?name where {
        ?what <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Journal> .
        ?what <http://www.w3.org/2000/01/rdf-schema#label> ?name .
        }""")
        try:
            rval = self.query()
            try:
                g = rval.convert()
            except:
                pass
            return [(x['name']['value'], x['what']['value']) for x in g['results']['bindings']]
        except:
            return None


    def get_journalless_articles(self):
        """
        Get list of VIVO articles which have no journal listed.
        :returns List of journalless articles (uri, PubMed Id)
        """
        self.setQuery("""select ?art ?pmid where {
           ?art <http://purl.org/ontology/bibo/pmid> ?pmid .
           filter not exists { ?art <http://vivoweb.org/ontology/core#hasPublicationVenue> ?o }
        }""")
        try:
            rval = self.query()
            try:
                g = rval.convert()
            except:
                pass
            return [(x['art']['value'], x['pmid']['value']) for x in g['results']['bindings']]
        except:
            return None


    def get_vcard(self, netid):
        """
        Find VCard for faculty member with given netid
        :param str netid: NetId of faculty member
        :returns URI of faculty member's VCard, or None
        """
        self.setQuery("""Select ?vcard where {
           ?who <http://vivo.dartmouth.edu/ontology/netId> "%s" .
           ?who <http://purl.obolibrary.org/obo/ARG_2000028> ?vcard .
        }""" % (netid))

        try:
            rval = self.query()
            try:
                g = rval.convert()
            except:
                pass
            return g['results']['bindings'][0]['vcard']['value']
        except:
            return None

    def get_identity(self, uid):
        """
        Get a profile's Last Name, First Name, and Email address.
        :param int uid: Geisel Id of faculty member.
        :returns dictionary with profile's last_name, first_name, and email
        """
        self.setQuery("""
        Select ?last ?first ?email where {
           ?who <http://vivo.dartmouth.edu/ontology/geiselId> \"""" + str(uid) + """"^^<http://www.w3.org/2001/XMLSchema#int> .
           ?who <http://purl.obolibrary.org/obo/ARG_2000028> ?id .
           ?id <http://www.w3.org/2006/vcard/ns#hasName> ?name .
           ?name <http://www.w3.org/2006/vcard/ns#familyName> ?last .
           ?name <http://www.w3.org/2006/vcard/ns#givenName> ?first .
           optional {
              ?id <http://www.w3.org/2006/vcard/ns#hasEmail> ?mail .
              ?mail <http://www.w3.org/2006/vcard/ns#email> ?email .
           }
        }""")
        rval = {'last_name': 'LAST',
                'first_name': 'FIRST',
                'email': 'EMAIL'
        }
        try:
            qval = self.query()
            try:
                g = qval.convert()
            except:
                pass
            rval['last_name'] = g['results']['bindings'][0]['last']['value']
            rval['first_name'] = g['results']['bindings'][0]['first']['value']
            rval['email'] = g['results']['bindings'][0]['email']['value']
        except:
            pass
        return rval

    def id_used(self, cid):
        """
        Check to see if a generic VIVO URI has been used.
        :param int cid: The URI's trailing number, gets appended to namespace.
        :returns True iff the URI has been used.
        """
        q = """
        Select ?x where {
           <%s/n%s> ?x ?y .
        } limit 1""" % (self._namespace, cid)
        self.setQuery(q)
        try:
            rval = self.query()
        except:
            print "Select failed"
            traceback.print_exc(file=sys.stdout)
            return True
        try:
            g = rval.convert()
            if len(g['results']['bindings'])<1:
                return False
        except:
            return True
        return True

    def get_available_id(self):
        """
        Find an available generic URI in VIVO.
        Try random values, and check if available.  Increase the number of
        random digits whenever we fail to find an open URI 5 times in a row.
        :returns int value of an unused VIVO generic URI.

        TODO: Is there a way to ask VIVO to supply an open URI?
        """
        tries = 0

        newid = self._digits + randrange(self._digits*9)
        while self.id_used(newid):
            tries = tries + 1
            if tries > 5:
                self._digits *= 10
                tries = 0
            newid = self._digits + randrange(self._digits*9)
        return newid

    def get_pmids(self, uid):
        """
        Get PubMed Id's authored by the given uid.
        :param int uid: GeiselId of faculty member
        :returns List of PubMed Id's of that faculty member's publications in VIVO.
        """
        self.setQuery("""
        Select ?pmid where {
           ?who <http://vivo.dartmouth.edu/ontology/geiselId> \"""" + str(uid) + """"^^<http://www.w3.org/2001/XMLSchema#int> .
           ?auth <http://vivoweb.org/ontology/core#relates> ?who .
           ?auth a <http://vivoweb.org/ontology/core#Authorship> .
           ?auth <http://vivoweb.org/ontology/core#relates> ?what .
           ?what a <http://purl.org/ontology/bibo/Article> .
           ?what <http://purl.org/ontology/bibo/pmid> ?pmid .
        }""")

        try:
            rval = self.query()
            g = rval.convert()
            return [x['pmid']['value'] for x in g['results']['bindings']]
        except:
            print "Select failed"
            traceback.print_exc(file=sys.stdout)

    def query_predicate(self, p):
        """
        Get all (Subject, Object) value pairs with the given Predicate.
        :param str p: Predicate to search for.
        :returns List of (Subject, Object) pairs with the given Predicate.
        """
        self.setQuery("""
        Select ?s ?o where {
        ?s %s ?o
        } ORDER BY (?s)""" % (p))

        try:
            rval = self.query()
            g = rval.convert()
            return [(x['s']['value'], x['o']['value']) for x in g['results']['bindings']]
        except:
            print "Select failed"
            traceback.print_exc(file=sys.stdout)



    def query_subject(self, s):
        """
        Get all (Predicate, Object) value pairs with the given Subject.
        :param str s: Subject to search for.
        :returns List of (Preicate, Object) pairs with the given Subject.
        """
        self.setQuery("""
        Select ?p ?o where {
        %s ?p ?o
        } ORDER BY (?p)""" % (s))

        try:
            rval = self.query()
            g = rval.convert()
            return [(x['p']['value'], x['o']['value']) for x in g['results']['bindings']]
        except:
            print "Select failed"
            traceback.print_exc(file=sys.stdout)


    def query_subject_raw(self, s):
        """
        Get all (Predicate, Object) pairs with the given Subject.
        :param str s: Subject to search for.
        :returns List of (Preicate, Object) pairs with the given Subject.
        """

        self.setQuery("""
        Select ?p ?o where {
        %s ?p ?o
        } ORDER BY (?p)""" % (s))

        try:
            rval = self.query()
            g = rval.convert()
            return [(x['p'], x['o']) for x in g['results']['bindings']]
        except:
            print "Select failed"
            traceback.print_exc(file=sys.stdout)


    def query_object_raw(self, o):
        """
        Get all (Subject, Predicate) pairs with the given Object.
        :param str o: Object to search for.
        :returns List of (Subject, Predicate) pairs with the given Object
        """
        self.setQuery("""
        Select ?s ?p where {
        ?s ?p %s
        } ORDER BY (?s)""" % (o))

        try:
            rval = self.query()
            g = rval.convert()
            return [(x['s'], x['p']) for x in g['results']['bindings']]
        except:
            print "Select failed"
            traceback.print_exc(file=sys.stdout)



    def get_all_netids(self):
        """
        Get list of all NetId's in VIVO
        """
        self.setQuery("""
        Select ?netid where {
           ?who <http://vivo.dartmouth.edu/ontology/netId> ?netid .
        }""")

        try:
            rval = self.query()
            g = rval.convert()
            return [x['netid']['value'] for x in g['results']['bindings']]
        except:
            print "Select failed"
            traceback.print_exc(file=sys.stdout)

    def get_bad_urls(self):
        q = """Select ?s ?o where {
        ?s <http://www.w3.org/2006/vcard/ns#url> ?o .
        }"""
        rs = self.doQuery(q)
        rval = []
        for row in rs:
            o = row['o']['value']
            if not (o.startswith('https://') or o.startswith('http://')):
                rval.append((row['s']['value'], o))
        return rval

    def get_excluded_pmids(self, uid):
        """
        Get list of PubMed Id's excluded from a given Profile
        :param int uid: GeiselId of faculty member
        :returns PubMed Id's excluded from the given faculty member.
        """
        self.setQuery("""
        Select ?pmid where {
           ?who <http://vivo.dartmouth.edu/ontology/geiselId> \"""" + str(uid) + """"^^<http://www.w3.org/2001/XMLSchema#int> .
           ?who <http://vivo.dartmouth.edu/ontology#excludePMID> ?pmid .
        }""")

        try:
            rval = self.query()
            g = rval.convert()
            return [x['pmid']['value'] for x in g['results']['bindings']]
        except:
            print "Select failed"
            traceback.print_exc(file=sys.stdout)

    def doQuery(self, s):
        """
        Execute a VIVO Sparql API query
        :param str s: The full query
        :returns The query's results
        """
        self.setQuery(s)

        try:
            rval = self.query()
            g = rval.convert()
            return g['results']['bindings']
        except:
            print "doQuery failed"
            traceback.print_exc(file=sys.stdout)

    def login(self):
        payload = {
            'loginName': self.user,
            'loginPassword': self.passwd,
            'loginForm': 'Log in'
        }
        self.session.post(self.vivo_url + 'authenticate',
                          data=payload,
                          verify=False)
        self.cookies = urllib.urlencode(self.session.cookies)

    def logout(self):
        resp = self.session.get(self.vivo_url + 'logout')
        logout_resp = resp.history[0]
        if logout_resp.status_code == 302:
            return True
        else:
            raise Exception('Logout failed.')

    def _query(self):
        """
        Override _query method to use cookies acquired on login.
        """
        request = self._createRequest()
        try:
            opener = urllib2.build_opener()
            opener.addheaders.append(('Cookie', self.cookies))
            response = opener.open(request)
            return (response, self.returnFormat)
        except urllib2.HTTPError, e:
            if e.code == 400:
                raise QueryBadFormed()
            elif e.code == 404:
                raise EndPointNotFound()
            elif e.code == 500:
                raise EndPointInternalError(e.read())
            else:
                raise e
            return (None, self.returnFormat)

    def disable_ssl(self):
        import ssl

        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            # Legacy Python that doesn't verify HTTPS certificates by default
            pass
        else:
            # Handle target environment that doesn't support HTTPS verification
            ssl._create_default_https_context = _create_unverified_https_context

    def __init__(self, vivo, account, pwd):
        self.disable_ssl()
        self._digits = 1000 # Start with 4 digit random URI's
        self.session = requests.session()
        self.vivo_url = vivo
        self.endpoint = vivo + 'admin/sparqlquery'
        SPARQLWrapper.__init__(self, self.endpoint)
        self.addParameter('resultFormat',
                                'application/sparql-results+json')
        self.setReturnFormat(JSON)
        if not pwd:
            pwd = getpass.getpass()

        self.setCredentials(account, pwd)
        self.login()
