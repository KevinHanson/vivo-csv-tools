# -*- coding: utf-8 -*-
"""
Controller logic for the finder application.

.. moduleauthor:: rdj
"""

import ConfigParser
from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.views.generic import DetailView
from import_tools.import_csv import add_authorship
from import_tools.import_csv import exclude_authorship
from vdm.catalyst import DisambiguationEngine
from vdm.pubmed import get_pubmed
from vivo_sparql.sparql_query import SparqlQuery


class SuggestView(DetailView):

    def import_pubmed(pmid):
        v = get_pubmed(pmid)

        ids = v["articleids"]
        idmap = {'pubmed': 'PubmedID', 'doi': 'DOI', 'mid': 'NIH-SSID',
                 'pmc': 'PubmedCentralID'}
        for i in idmap:
            v[i] = ''

        for i in ids:
            try:
                if idmap[i['idtype']]:
                    v[i['idtype']] = i['value']
            except:
                pass

        authors = v["authors"]
        astring = ''
        for author in authors:
            if astring:
                astring += ", "
            astring += author['name']

        pages = v["pages"].split('-')

        start_page = pages[0]
        if len(pages) < 2:
            end_page = ""
        else:
            end_page = pages[1]

        info = {}
        info['pubmed'] = v['pubmed']
        info['pmc'] = v['pmc']
        info['doi'] = v['doi']
        info['mid'] = v['mid']
        info['title'] = v['title']
        info['year'] = v['pubdate'].split()[0]
        info['authors'] = astring
        info['journal'] = v['fulljournalname']
        info['issue'] = v['issue']
        info['volume'] = v['volume']
        info['start_page'] = start_page
        info['end_page'] = end_page

        return info

    def get_context(self, request, uid):
        pmids = None
        if request.session.get('_uid')==uid:
            pmids = request.session.get('_pmids')

        config = ConfigParser.ConfigParser()
        config.read("vivo_csv.cfg")

        s = SparqlQuery(config.get("main", "vivo_url"),
                        config.get("main", "vivo_account"),
                        config.get("main", "vivo_pwd"))
        ident = s.get_identity(uid)
        ident['UID'] = uid

        if not pmids:
            known = s.get_pmids(uid)
            excluded = s.get_excluded_pmids(uid)
            de = DisambiguationEngine()
            pmids = de.do(ident['first_name'], ident['last_name'], "", ident['email'], known, excluded)
            pmids = [x for x in pmids if x not in known]


        s.logout()
        if len(pmids)==0:
            pmids = [0]
            article = None
        else:
            article = import_pubmed(pmids[0])

        request.session['_pmids'] = pmids[1:]
        request.session['_uid'] = uid

        context = {}
        context['art'] = pmids[0]
        context['identity'] = ident
        context['info'] = article
        context['count'] = len(pmids)

        return context

    def get(self, request, uid, *args, **kwargs):
        context = self.get_context(request, uid)
        return render(request, "suggest.html", context)

    def post(self, request, *args, **kwargs):
        uid = request.session['_uid']
        pub = request.POST['submit']
        if pub.startswith('show'):
            pmid = int(pub[4:])
            add_authorship(uid, pmid)
        elif pub.startswith('hide'):
            pmid = int(pub[4:])
        elif pub.startswith('no'):
            pmid = int(pub[2:])
            exclude_authorship(uid, pmid)
            del request.session['_pmids']
            request.session['_uid'] = ''


        context = self.get_context(request, uid)

        return render(request, "article.html", context)
