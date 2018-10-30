#!/usr/bin/env python
# coding: utf-8

import re
import time
import datetime
import codecs
import csv
import json
import httplib
import urllib2
import logging
import traceback
import argparse
import random
from threading import Thread, RLock
import socket
socket.setdefaulttimeout(10.0)


BASE_URL = 'https://api.elsevier.com'
API_KEY = [
]

URL_MAPPING = {
    'ScopusSearch': 'content/search/scopus',
    'serial_title': 'content/serial/title',
    'article_eid': 'content/abstract/eid',
    'ScopusRetrieval': 'content/abstract/scopus_id',
    'AuthorSearch': 'content/author/author_id'
}


QUERY_FIELDS_INDEX = {
    'doi': 11,
    'eid': 19,
    'issn': 0
}

TITLE_MAPPING = {
    'references': [
        'subject fields', 'ref:title', 'source id', 'ref:EID', 'Year', 'Affiliation Country', 'SubType',
        'Document Type', 'ref: subject fields', 'citedby-count', 'issn',
    ],
    'author_info': [
        'Sum DC', 'DC/au', 'DC/y', 'Sum CBC', 'CBC/au', 'CBC/y', 'Sum CC', 'CC/au', 'CC/y', 'Avg Year', 'au num', 'Author ID', 'document-count', 'au: dc/year', 'cited-by-count', 'au: cbc/year', 'citation-count', 'au: cc/year',
        'Publiction Start', 'Publiction End', 'SubType',
        'Affiliation Country Current', 'Affiliation Country History'
    ],
    'author_citation': [
        'Author ID', 'document-count', 'cited-by-count', 'citation-count', 'Publiction Start', 'Publiction End', 'SubType',
        'Affiliation Country Current', 'Affiliation Country History'
    ]
}

FIELDS = [
    'citation_count',
    'citations_overview'
]

current_key_index = 0
used_token_index = [0]

HEADERS = {
    'X-ELS-APIKey': API_KEY[current_key_index],
    'Accept': 'application/json'
}

search_year = []

log_name = 'err-' + datetime.date.today().strftime('%Y-%m-%d') + '.log'
logging.basicConfig(filename=log_name, level=logging.DEBUG)

mutex = RLock()


class ScopusAPI(object):
    def __init__(self):
        self.base_url = BASE_URL
        self.headers = HEADERS

    def get_data(self, data, writer):
        pass

    def get_article_info_by_eid(self, eid):
        url = self.get_url(eid, 'article_eid')
        resp = self.get_resp(url)
        return resp

    def get_res(self, url):
        res = None
        try:
            req = urllib2.Request(url=url, headers=self.headers)
            res = urllib2.urlopen(req, timeout=10)
        except socket.timeout:
            print "Time out when fectching data....retrying...."
        return res

    def get_resp(self, url):
        count = 0
        while count < 4:
            count += 1
            try:
                res = self.get_res(url)
                if res.code == 200:
                    try:
                        return json.loads(res.read())
                    except httplib.IncompleteRead as e:
                        logging.error('[%s]: %s' % (datetime.datetime.now(), traceback.format_exc()))
                        continue
            except urllib2.HTTPError as e:
                mutex.acquire()
                logging.error('[%s]: %s' % (datetime.datetime.now(), traceback.format_exc()))
                global current_key_index, used_token_index
                while current_key_index in used_token_index:
                    current_key_index = random.randint(0, len(API_KEY) - 1)
                used_token_index.append(current_key_index)
                self.headers['X-ELS-APIKey'] = API_KEY[current_key_index]
                mutex.release()
            except:
                traceback.print_exc()
        return None

    @classmethod
    def get_sub_areas(cls, data):
        subareas = data.get('subject-areas', {}) or {}
        subareas = subareas.get('subject-area', []) or []
        if not isinstance(subareas, list):
            logging.info('[%s subareas error]: %s' % (datetime.datetime.now(), subareas))
            subareas = [subareas]
        sub_list = []
        for sub in subareas:
            if isinstance(sub, dict):
                sub_list.append(sub.get('@abbrev'))
            if isinstance(sub, list):
                for s in sub:
                    sub_list.append(s.get('@abbrev'))
        return list(set(sub_list))

    @staticmethod
    def get_scoups_bibrecord(data):
        ab_re_resp = data.get('abstracts-retrieval-response', {})
        data_item = ab_re_resp.get('item', {}) or {}
        bibrecord = data_item.get('bibrecord', {}) or {}
        return bibrecord

    def get_url(self, id='', data_type='ScopusRetrieval'):
        url = '/'.join([self.base_url, URL_MAPPING.get(data_type), id])
        return url

    def get_scopus_id_by_eid(self, eid):
        li = eid.split('0-')
        scopus_id = li[-1] if len(li) == 2 else ''
        return scopus_id

    def get_head(self, resp_data):
        bibrecord = self.get_scoups_bibrecord(resp_data)
        head = bibrecord.get('head', {})
        return head


class SerialTitleSearch(ScopusAPI):
    def get_serial_title(self, query_field, line):
        query = line[QUERY_FIELDS_INDEX.get(query_field)]
        url = '/'.join([self.base_url, URL_MAPPING['serial_title']]) + '?' + 'issn=' + query
        req = urllib2.Request(url=url, headers=self.headers)
        res = urllib2.urlopen(req)
        data = ''
        if res.code == 200:
            resp = json.loads(res.read())
            data = resp.get('serial-metadata-response', {}) or {}
            data = data.get('entry')
        return data


class ReferenceSearch(ScopusAPI):

    def get_ref_list_by_eid(self, resp_data):

        def get_id(item):
            try:
                d = item.get('ref-info', {}).get('refd-itemidlist', {}).get('itemid', {})
                if isinstance(d, list):
                    for i in d:
                        if re.match(r'[0-9]', i.get('$')):
                            d = i
                d = d.get('$')
            except Exception as e:
                logging.info('[date error] %s' % item)
                logging.info('[date error] %s' % e)
                d = None
            return d

        bibrecord = self.get_scoups_bibrecord(resp_data)
        tail_data = bibrecord.get('tail', {}) or {}
        bibliography = tail_data.get('bibliography', {}) or tail_data
        reference = bibliography.get('reference', [])
        ref_ids = [get_id(item) for item in reference]
        sub_fields = self.get_sub_areas(bibliography)
        return {'sub_fields': sub_fields, 'ref_ids': ref_ids}

    def get_ref_ids_by_eid(self, eid):
        resp = self.get_article_info_by_eid(eid)
        data = {}
        if resp:
            data = self.get_ref_list_by_eid(resp)
        return data

    def get_author_keywords(self, head_data):
        citation_info = head_data.get('citation-info', {})
        keywords = citation_info.get('author-keywords', {}).get('author-keyword', [])
        return [kw.get('$', '') for kw in keywords]

    def get_scopus_info(self, scopus_id):
        ref_info = {}
        if not scopus_id:
            return None
        data = self.get_scopus_origin_data(scopus_id)
        if data:
            data = data.get('abstracts-retrieval-response', {})
            affiliation = data.get('affiliation', [])
            if isinstance(affiliation, dict):
                affiliation = [affiliation]
            aff_country = ','.join(list(set([aff.get('affiliation-country') for aff in affiliation])))
            core_data = data.get('coredata', {})
            sub_fields = ','.join(self.get_sub_areas(data))
            head_data = self.get_head(data)
            ref_year = 1900
            try:
                ref_year = datetime.datetime.strptime(core_data.get('prism:coverDate', ''), '%Y-%m-%d').year
            except ValueError as e:
                logging.info('[date error] %s' % core_data)
                logging.error(traceback.format_exc())
            ref_info = {
                'affiliation country': aff_country,
                'subject arears': sub_fields,
                'docType': core_data.get('srctype', ''),
                'issueIdentifier': core_data.get('issueIdentifier', 0),
                'eid': core_data.get('eid', ''),
                'coverDate': core_data.get('prism:coverDate', ''),
                'year': ref_year,
                'aggregationType': core_data.get('prism:aggregationType', ''),
                'url': core_data.get('prism:url', ''),
                'subtype': core_data.get('subtype', ''),
                'subtypeDescription': core_data.get('subtypeDescription', ''),
                'publicationName': core_data.get('prism:publicationName', ''),
                'source-id': core_data.get('source-id', ''),
                'citedby-count': core_data.get('citedby-count', 0),
                'volume': core_data.get('prism:volume'),
                'pageRange': core_data.get('prism:pageRange'),
                'title': core_data.get('dc:title', ''),
                'endingPage': core_data.get('prism:endingPage'),
                'openaccess': core_data.get('openaccess', ''),
                'openaccessFlag': core_data.get('openaccessFlag', ''),
                'doi': core_data.get('prism:doi', ''),
                'issn': core_data.get('prism:issn', ''),
                'startingPage': core_data.get('prism:startingPage', ''),
                'scopus_id': core_data.get('dc:identifier', ''),
                'publisher': core_data.get('dc:publisher', ''),
                'author_keywords': core_data.get('dc:publisher', ''),
                'creator': ','.join([a.get('ce:indexed-name', '') for a in core_data.get('dc:creator', {}).get('author', [])]),
                'author_words': self.get_author_keywords(head_data)
            }
        return ref_info

    def get_scopus_origin_data(self, scopus_id):
        url = self.get_url(scopus_id, 'ScopusRetrieval')
        return self.get_resp(url)

    def get_aggregate(self, references):
        global search_year
        ret = {}
        for s_year in search_year:
            year_count = [ref for ref in references if ref.get('year') == int(s_year) and ref.get('affiliation country')]
            co = len([ref for ref in year_count if ref.get('affiliation country').lower() == 'china'])
            nc = len([ref for ref in year_count if 'china' not in ref.get('affiliation country').lower()])
            ret.update({
                s_year: {
                    'target_year': len(year_count),
                    'co': co,
                    'nc': nc
                }
            })
        return ret

    def get_ref_info_for_export(self, rid, res_list, ref_info_list):
        ref_info = {}
        res = []
        try:
            ref_info = self.get_scopus_info(rid)
            res = [''] * 21
            res.extend([
                ref_info.get('title', '') or ref_info.get('publicationName', ''),
                ref_info.get('source-id', ''),
                ref_info.get('eid', ''),
                str(ref_info.get('year', '')),
                ref_info.get('affiliation country', ''),
                ref_info.get('subtypeDescription', ''),
                ref_info.get('aggregationType', ''),
                ref_info.get('subject arears', ''),
                ref_info.get('citedby-count', ''),
                ref_info.get('issn', ''),
            ])
            res = [s.encode('utf-8') for s in res]
        except Exception as e:
            logging.info('[%s EID: %s]: %s' % (datetime.datetime.now(), rid, e))
            logging.error(traceback.format_exc())
        res_list.append(res)
        ref_info_list.append(ref_info)

    def get_ref(self, src_data, f_writer):
        eid = src_data[QUERY_FIELDS_INDEX.get('eid')]
        print eid
        if not eid:
            return
        main_data = self.get_ref_ids_by_eid(eid)
        ref_ids = main_data.get('ref_ids', []) or []
        sub_fields = main_data.get('sub_fields') or []
        try:
            src_data.append(','.join(sub_fields))
        except:
            logging.info('[%s subfields error]: %s' % (datetime.datetime.now(), sub_fields))
            logging.error(traceback.format_exc())
            src_data.append('')
        res_list = []
        ref_agg_list = []
        thread_list = []
        count = 0
        for rid in ref_ids:
            count += 1
            if count % 5 == 0:
                time.sleep(1)
            if not rid:
                continue
            try:
                t = Thread(target=self.get_ref_info_for_export, args=(rid, res_list, ref_agg_list))
                t.setDaemon(True)
                t.start()
                thread_list.append(t)
            except Exception as e:
                logging.info('[%s EID: %s]: %s' % (datetime.datetime.now(), rid, e))
                logging.error(traceback.format_exc())
        for th in thread_list:
            try:
                th.join()
            except IndexError as e:
                continue
        ref_agg = self.get_aggregate(ref_agg_list)
        agg_append = [''] * 10
        global search_year
        for y in search_year:
            yd = ref_agg.get(y)
            agg_append.extend([
                yd.get('target_year'),
                yd.get('co'),
                yd.get('nc'),
            ])
        src_data.extend(agg_append)
        try:
            f_writer.writerow(src_data)
            f_writer.writerows(res_list)
        except Exception as e:
            traceback.print_exc()
            logging.info('[%s]: %s' % (datetime.datetime.now(), src_data))
            logging.error(traceback.format_exc())

    def get_data(self, data, writer):
        self.get_ref(data, writer)


class AuthorSearch(ScopusAPI):
    ori_extend_num = 11

    def get_authors_from_article(self, article_data):
        if not article_data:
            return []
        bibrecord = self.get_scoups_bibrecord(article_data)
        head = bibrecord.get('head', {})
        author_group = head.get('author-group', {})
        authors = []
        if isinstance(author_group, dict):
            authors = author_group.get('author', []) or []
        if isinstance(author_group, list):
            for ag in author_group:
                authors.extend(ag.get('author', []) or [])
        auid_list = [au.get('@auid') if au and isinstance(au, dict) else '' for au in authors]
        return auid_list

    def get_aff_country(self, profile):
        def get_aff_country(aff):
            if isinstance(aff, dict):
                return aff.get('ip-doc', {}).get('address', {}).get('country', '')
            if isinstance(aff, list):
                return ','.join(list(set([item.get('ip-doc', {}).get('address', {}).get('country', '') for item in aff])))
            return ''

        return {
            'current': get_aff_country(profile.get('affiliation-current', {}).get('affiliation')),
            'history': get_aff_country(profile.get('affiliation-history', {}).get('affiliation'))
        }

    def get_author_profile(self, author_id):
        if not author_id:
            return {}
        url = self.get_url(author_id, 'AuthorSearch')
        resp = self.get_resp(url)
        return self.get_author_profile_data(resp)

    def get_author_profile_data(self, author_data):
        author_info = {}
        if not author_data:
            return {}
        ar_resp = author_data.get('author-retrieval-response', [])
        if len(ar_resp) == 1:
            au_data = ar_resp[0]
            core_data = au_data.get('coredata', {})
            sub_area_list = self.get_sub_areas(au_data)
            author_profile = au_data.get('author-profile', {})
            publication_range = author_profile.get('publication-range', {})
            preferred_name = author_profile.get('preferred-name', {})
            aff_info = self.get_aff_country(author_profile)
            author_info = {
                'eid': core_data.get('eid', ''),
                'document-count': core_data.get('document-count', ''),
                'cited-by-count': core_data.get('cited-by-count', ''),
                'citation-count': core_data.get('citation-count', ''),
                'dc:identifier': core_data.get('dc:identifier', ':').split(':')[1],
                'sub_areas': sub_area_list,
                'pub_start': publication_range.get('@start', ''),
                'pub_end': publication_range.get('@end', ''),
                'first_name': preferred_name.get('given-name', ''),
                'last_name': preferred_name.get('surname', ''),
                'index_name': preferred_name.get('indexed-name', ''),
                'affilication-current': aff_info.get('current', ''),
                'affilication-history': aff_info.get('history', ''),
            }
        return author_info

    def get_authors_by_scopus_eid(self, eid):
        print eid
        resp = self.get_article_info_by_eid(eid)
        results = []
        if not resp:
            return results
        auids = self.get_authors_from_article(resp)
        thread_list = []
        count = 0
        for auid in auids:
            count += 1
            if count % 5 == 0:
                time.sleep(1)
            if not auid:
                continue
            try:
                t = Thread(target=self.get_author_info, args=(auid, results))
                t.setDaemon(True)
                t.start()
                thread_list.append(t)
            except Exception as e:
                logging.info('[%s EID: %s]: %s' % (datetime.datetime.now(), eid, e))
                logging.error(traceback.format_exc())
        for th in thread_list:
            try:
                th.join()
            except IndexError as e:
                continue
        return results

    def get_author_info(self, author_id, res_list):
        if not author_id:
            return []
        profile = self.get_author_profile(author_id)
        res = []
        year_length = float(profile.get('pub_end', '')) - float(profile.get('pub_start', '')) + 1.0
        if not year_length:
            year_length = 1
        try:
            res.extend([
                profile.get('dc:identifier', ''),
                profile.get('document-count', ''),
                str(float(profile.get('document-count', '')) / year_length),
                profile.get('cited-by-count', ''),
                str(float(profile.get('cited-by-count', '')) / year_length),
                profile.get('citation-count', ''),
                str(float(profile.get('citation-count', '')) / year_length),
                profile.get('pub_start', ''),
                profile.get('pub_end', ''),
                ';'.join(profile.get('sub_areas', [])),
                profile.get('affilication-current', []),
                profile.get('affilication-history', []),
            ])
            res = [s.encode('utf-8') for s in res]
        except Exception as e:
            logging.info('[%s AUTHOR_ID: %s]: %s' % (datetime.datetime.now(), author_id, e))
            logging.error(traceback.format_exc())
        res_list.append(res)
        return res_list

    def get_author_scopus_info(self, author_id):
        query_str = '?query=AU-ID(%s)&field=dc:identifier,prism:coverDisplayDate' % author_id
        url = self.get_url('', 'ScopusSearch') + query_str
        resp = self.get_resp(url)
        return resp

    def get_data(self, data, writer):
        au = self.get_authors_by_scopus_eid(data[QUERY_FIELDS_INDEX.get('eid')])
        res_list = []
        space = [''] * (len(data) + self.ori_extend_num)
        for res in au:
            res_list.append(space.extend(res))
        author_num = len(res_list)
        y = dc = cbc = cc = 0.0
        for au in res_list:
            dc += float(au[len(data) + self.ori_extend_num + 1])
            cbc += float(au[len(data) + self.ori_extend_num + 3])
            cc += float(au[len(data) + self.ori_extend_num + 5])
            y += float(au[len(data) + self.ori_extend_num + 8]) - float(au[len(data) + self.ori_extend_num + 7]) + 1.0
        if not author_num:
            author_num = 1
        if not y:
            y = 1

        data.extend([
            str(dc), str(dc/author_num), str(dc/y),
            str(cbc), str(cbc/author_num), str(cbc/y),
            str(cc), str(cc/author_num), str(cc / y),
            str(y/author_num), str(author_num)
        ])
        try:
            writer.writerow(data)
            writer.writerows(res_list)
        except Exception as e:
            traceback.print_exc()
            logging.info('[%s]: %s' % (datetime.datetime.now(), data))
            logging.error(traceback.format_exc())


class AuthorRef(ScopusAPI):

    def __init__(self):
        self.author_api = AuthorSearch()
        self.ref_api = ReferenceSearch()
        super(AuthorRef, self).__init__()

    def get_author_ref(self, data):
        res = []
        data_len = 2
        authors = self.author_api.get_authors_from_article(data)
        for auid in authors:
            au_data = [''] * data_len
            au_info = self.author_api.get_author_profile(auid)
            au_data.extend([
                au_info.get('dc:identifier', ''),
                au_info.get('index_name', ''),
                au_info.get('affilication-current', ''),
                ';'.join(au_info.get('sub_areas', [])),
            ])
            res.append(au_data)

    def get_citation_resp(self, scopus_id, res_list):
        pub_data = self.ref_api.get_scopus_info(scopus_id)



    def get_author_publication(self, author_id):
        au_resp = self.author_api.get_author_scopus_info(author_id)
        au_pubs = au_resp.get('search-results', {}).get('entry', [])
        pub_info = []
        for pub in au_pubs:
            pub_date = pub.get('prism:coverDate', '').split('-')
            pub_year = 0
            if len(pub_date) > 0:
                pub_year = int(pub_date[0])
            if search_year and pub_year not in search_year:
                continue
            pub_url = pub.get('prism:url', '')
            pub_res_data = []
            pub_thread = Thread(target=self.get_resp, args=(pub_url,))

    def get_ref_by_pub(self):
        pass

    def cal_author(self):
        pass

    def cal_keywords(self):
        pass

    def cal_subarea(self):
        pass

    def get_data(self, data, writer):
        pass


class Search(object):
    def __init__(self, filename, start, end, query='reference'):
        self.base_url = BASE_URL
        self.csv_file = open(filename, 'r')
        self.reader = csv.reader(self.csv_file)
        suffix = str(start) if start else '1'
        if end:
            suffix += '-' + str(end)
        result_file_name = '-'.join([filename.split('.')[0], suffix, query, 'result.csv'])
        self.result_file = open(result_file_name, 'w')
        self.title = []
        self.data_type = query
        self.start_line = start
        self.end_line = end
        if query == 'references':
            self.api = ReferenceSearch()
        if query == 'author_info':
            self.api = AuthorSearch()

    def load_data(self):
        for line in self.reader:
            if self.reader.line_num == 1:
                self.title = line
                continue
            yield line

    def run(self):
        ori_data = self.load_data()
        with self.result_file as f:
            f.write(codecs.BOM_UTF8)
            writer = csv.writer(f)
            w_title = False
            count = 0
            for line in ori_data:
                if not w_title:
                    self.title.extend(TITLE_MAPPING[self.data_type])
                    if self.data_type == 'references':
                        global search_year
                        for x in search_year:
                            self.title.extend(['Y' + x, 'CO', 'NC'])
                    writer.writerow(self.title)
                    w_title = True
                count += 1
                if self.start_line > 1:
                    if count < self.start_line:
                        continue
                if self.end_line:
                    if count > self.end_line:
                        break
                self.api.get_data(line, writer)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, help='file name')
    parser.add_argument('--start', type=int, help='start line')
    parser.add_argument('--end', type=int, help='end line')
    parser.add_argument('--year', type=str, help='count year')
    parser.add_argument('--type', type=str, help='count year')
    args = parser.parse_args()
    src_file = args.file
    start = args.start
    end = args.end
    query_type = args.type or 'references'
    input_year = args.year.split(',') if args.year else ['2013']
    if not src_file:
        print 'please input filename'
    else:
        global search_year
        search_year = input_year or [2013]

        sapi = Search(src_file, start, end, query_type)
        sapi.run()
