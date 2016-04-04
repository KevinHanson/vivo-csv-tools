import unittest
import filecmp
import os
from rdflib import term
import sys
from template_graph import TemplateGraph

class TestInternals(unittest.TestCase):

    def test_update_namespace(self):
        g = TemplateGraph('test.cfg', 'people')
        g._read_config()
        assert(g._update_namespace('http://vivo.dartmouth.edu/individual/xyz')=='http://destination.dartmouth.edu/vivo/individual/xyz')

    def toast_new_uri(self):
        g = TemplateGraph('test.cfg', 'people')
        g._read_config()
        g._init_sparql()
        g._uri_map = {}
        uri = g._uri('http://vivo.dartmouth.edu/individual/n1234')
        assert uri.startswith('http://destination.dartmouth.edu/vivo/individual/nn')
        uri2 = g._uri('http://vivo.dartmouth.edu/individual/n12345')
        uri3 = g._uri('http://vivo.dartmouth.edu/individual/n1234')
        assert(uri!=uri2)
        assert(uri==uri3)

    def test_substitutions(self):
        g = TemplateGraph('test.cfg', 'people')
        g._read_config()
        g._row = ['one', 'two', 'three']

        trm = term.URIRef('http://somewhere.edu/$VAR001x')
        s = g._fill_in(trm)
        assert(s==term.URIRef('http://somewhere.edu/twox'))

        s = g._replace(term.URIRef('http://somewhere.edu/per987-x'), '987', 3)

        assert(s==term.URIRef('http://somewhere.edu/per$VAR003-x'))

    def compare_files(self, filename1, filename2):
        with open(filename1) as f:
            s1 = f.readlines()
            s1.sort()
        with open(filename2) as f:
            s2 = f.readlines()
            s2.sort()

        if s1==s2:
            return True
        return False

    def import_export(self, section):
        print "io", section
        g = TemplateGraph('test.cfg', section)
        g.import_csv()
        g.export_csv('testdata/export.csv')
        assert(self.compare_files('testdata/export.csv', 'testdata/expected/%s.csv' % section))

    def test_import_export(self):
        sections = ['departments', 'people', 'articles']
        for section in sections:
            self.import_export(section)

if __name__ == '__main__':
    unittest.main()
