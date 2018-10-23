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
    'f39255b9bcee3c57c25fcee73fe3b1eb',
    'c5e8499f9cd003ab7f2491f0994e6aae',
    '915aa4e98d87db08fc438373ecf6c111',
    '04a087d08f7eff51e081a07d8e8818c6',
    'ff09db2296a083be41109ddab506d1bd',
    'fa23e19c8245d00e3b2c616778dd47ac',
    'ea957469be9a37c7374843f716da6da5',
    'f61f798bc06230fac1f1dbd65a947812',
    '8672a6f512c6844ae11009c62083234b',
    '63760081141693255fccff56c56601de',
    '17d75a74353d609b1cf88e15953e7476',
    'b680ec38b8ee3f7b08cc872980a95f47',
    '798abcb0627d7d2e5ef8bd52467a241b',
    '6f9d341e51e371fd28130f9c09de910a',
    '075458d396d7fd71dc7b261dd5e9843f',
    '375e902bfc06dc187ae58e422737c194',
    '491544b1d1875b05f309832020212fae',
    '8de77bb4d9761abc10b67f72dbc20482',
    '5e12e963d34cbee4ae0a1943bd3a3ea3',
    'b262c0592a75f66b52468d50ac3a9a26',
    'c65b79b4b6de2cacdf88cdd756bf795d',
    'dba33988d35feb3021c0e9931ff85fd6',
    '03fc7c9c4913f1d716f07ea17c4af085',
    'd71b2ae624f0d52002d1ef281d559a25',
    'c052bfc5ee22625826e6b824d2307631',
    '7a94350be4db8d485856058ad4ab9f86',
    'fa022044b782fbf190a8ec448e9899df',
    '5be6577c1126599a69b08c690c0c52ae',
    '0e597d281b2d93b39b82716dfc17a8de',
    '347ac7494dcfd4b1536a6ead0cb3f758',
    'c06addca9cb36ad68ea16fc519fbfd81',
    'b67aede30ea8af5a15ef7488eaddf3fb',
    '93ab950141f1fab6b19305a341dd9981',
    'c855762680d3ffb0cca63f7a2fb18768',
    '9751d6306174e054c83414ed5e1fe28d',
    'd1ae02b0facf9154fec219e9806a55d1',
    '516a38f61002997fa9296f0314bce5b8',
    '22bcec8b89ded0aff80e32d1a97c99c5',
    '555f473aa39077d8fcb2a49530d7874a',
    '38b92e84e020b48890b27c433756a697',
    '2bd42b9b5b34b675cf7fd9e07754208a',
    '42581e88115978e1a9e4e0ca03485904',
    '8f435b5326a47024cd5706e2971ca891',
    'dace6b7365c0a83861e5d8c83f41131e',
    'd75b338eb9ea21990873bc2130008875',
    '1dc24671c3f1f73e41bd30fac5798949',
    'fcacf0d3b42ca665079af50052a35ed3',
    '36391cf22e6e523b0081eef679a27ae8',
    '1e2922ffe08765518081f0d75161eb84',
    '5a9423d1e5172f941543177542cfbb57',
    '51e0ce7155124182a1b0099b71b8f6a7',
    '5388631052a301e869e7bc78f7abf96d',
    '6048a7c1bdbe04791eda89ccbacc6121',
    '8d769014358a38ad12d797260560359d',
    '900e0a75bc1d99218027b1ff661d3e4a',
    '84f60d706011e5127c6f697ea4af2e50',
    'e6b8ba28b0dcab1d7098e635bbf28595',
    'be9a5ee85995687e5c73b4dade005f18',
    'c2f2fbc63369236590811ed1109cc17d',
    '66a9efc9bfc7723ecbad6b7402173000',
    'b826194a1b11adb9aa3a21919e7642ac',
    '19d902e73182b6f061b27141a4dfefbe',
    '9abe1b8202ce4d51751fe8577cf013a1',
    '1c2fa5c22295164647d6fb879b1b8172',
    '6757f56a44c489280f4a8cdbe5648c5f',
    'fc502b10d8ea42110f88c133d3783b6c',
    '26b386724c465bf301895cfb424ec0cd',
    'e0f47588b1c44ac827108a0b05c28819',
    'f764990922b47fdb028fcf513d4a040d',
    '767594361ac4c640ff598d94dcbecd37',
    '8c32fbff69e7e4e05f124d69d7ab2c54',
]

URL_MAPPING = {
    # 'ScopusSearch': 'content/search/scopus',
    'serial_title': 'content/serial/title',
    'article_eid': 'content/abstract/eid',
    # 'AbstractScopus': 'content/abstract/scopus_id',
    'ScopusSearch': 'content/abstract/scopus_id',
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
    'author': [
        'subject fields', 'ref:title', 'source id', 'ref:EID', 'Year', 'Affiliation Country', 'SubType',
        'Document Type', 'ref: subject fields', 'citedby-count', 'issn',
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

    def get_url(self, id='', data_type='ScopusSearch'):
        url = '/'.join([self.base_url, URL_MAPPING.get(data_type), id])
        return url

    def get_scopus_id_by_eid(self, eid):
        li = eid.split('0-')
        scopus_id = li[-1] if len(li) == 2 else ''
        return scopus_id

    # def run(self, data_type, start_num=1, end_num=None):
    #     ori_data = self.load_data()
    #     with self.result_file as f:
    #         f.write(codecs.BOM_UTF8)
    #         writer = csv.writer(f)
    #         w_title = False
    #         count = 0
    #         for line in ori_data:
    #             if not w_title:
    #                 self.title.extend(TITLE_MAPPING[data_type])
    #                 global search_year
    #                 for x in search_year:
    #                     self.title.extend(['Y' + x, 'CO', 'NC'])
    #                 writer.writerow(self.title)
    #                 w_title = True
    #             count += 1
    #             if start_num > 1:
    #                 if count < start_num:
    #                     continue
    #             if end_num:
    #                 if count > end_num:
    #                     break
    #             if data_type == 'references':
    #                 self.get_ref(line, writer)
    #             if data_type == 'serial_title':
    #                 self.get_serial_title('issn', line)
    #             if data_type == 'author':
    #                 self.get_authors_by_scopus_eid(line[QUERY_FIELDS_INDEX.get('eid')])


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

    def get_ref_info(self, r_scopus_id):
        ref_info = {}
        if not r_scopus_id:
            return None
        data = self.get_scopus_info(r_scopus_id)
        if data:
            data = data.get('abstracts-retrieval-response', {})
            affiliation = data.get('affiliation', [])
            if isinstance(affiliation, dict):
                affiliation = [affiliation]
            aff_country = ','.join(list(set([aff.get('affiliation-country') for aff in affiliation])))
            core_data = data.get('coredata', {})
            sub_fields = ','.join(self.get_sub_areas(data))
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
                'creator': ','.join([a.get('ce:indexed-name', '') for a in core_data.get('dc:creator', {}).get('author', [])]),
            }
        return ref_info

    def get_scopus_info(self, scopus_id):
        url = self.get_url(scopus_id, 'ScopusSearch')
        print url
        return self.get_resp(url)

    def get_aggregate(self, references):
        global search_year
        ret = {}
        for s_year in search_year:
            year_count = [ref for ref in references if ref.get('year') == int(s_year) and ref.get('affiliation country')]
            co = len([ref for ref in year_count if ref.get('affiliation country').lower() == 'china'])
            nc = len([ref for ref in year_count if 'china' not in ref.get('affiliation country').lower()])
            ret.update({
                s_year:{
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
            ref_info = self.get_ref_info(rid)
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
            # self.get_ref_info_for_export(rid, res_list, ref_agg_list)
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
    def get_authors_from_article(self, article_data):
        if not article_data:
            return []
        bibrecord = self.get_scoups_bibrecord(article_data)
        head = bibrecord.get('head', {})
        authors = head.get('author-group', {}).get('author', []) or []
        # au_list = []
        # for item in authors:
        #     au_list.extend(item.get('author', []))
        auid_list = [au.get('@auid') if au and isinstance(au, dict) else '' for au in authors]
        return auid_list

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
            print preferred_name
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
            }
        return author_info

    def get_authors_by_scopus_eid(self, eid):
        resp = self.get_article_info_by_eid(eid)
        results = []
        if resp:
            auids = self.get_authors_from_article(resp)
            for auid in auids:
                results.append(self.get_author_info(auid))
        return results

    def get_author_info(self, author_id):
        if not author_id:
            return
        profile = self.get_author_profile(author_id)
        publications = self.get_author_scopus_info(author_id)
        print profile
        print '####'
        print publications
        return {
            'profile': profile,
            'publications': publications,
        }

    def get_author_scopus_info(self, author_id):
        query_str = '?query=AU-ID(%s)&field=dc:identifier,prism:coverDisplayDate' % author_id
        url = self.get_url('', 'ScopusSearch') + query_str
        resp = self.get_resp(url)
        print resp
        return resp

    def get_data(self, data, writer):
        self.get_authors_by_scopus_eid(data[QUERY_FIELDS_INDEX.get('eid')])


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
            # self.get_ref(line, writer)
            self.api = ReferenceSearch()
        # if data_type == 'serial_title':
        #     self.get_serial_title('issn', line)
        if query == 'author':
            # self.get_authors_by_scopus_eid(line[QUERY_FIELDS_INDEX.get('eid')])
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
                # if data_type == 'references':
                #     self.get_ref(line, writer)
                # if data_type == 'serial_title':
                #     self.get_serial_title('issn', line)
                # if data_type == 'author':
                #     self.get_authors_by_scopus_eid(line[QUERY_FIELDS_INDEX.get('eid')])


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
