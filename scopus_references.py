#!/usr/bin/env python
# coding: utf-8

import time
import datetime
import codecs
import sys
import csv
import json
import httplib
import urllib2
import logging
import sys
import traceback
import argparse
from threading import Thread


BASE_URL = 'https://api.elsevier.com'
API_KEY = [
]

URL_MAPPING = {
    'scopus_id': 'content/search/scopus',
    'serial_title': 'content/serial/title',
    'ref_id': 'content/abstract/eid',
    'scopus': 'content/abstract/scopus_id'
}


QUERY_FIELDS_INDEX = {
    'doi': 11,
    'eid': 19,
    'issn': 0
}

TITLE_MAPPING = {
    'references': [
        'subject fields', 'ref:title', 'source id', 'ref:EID', 'Year', 'Affiliation Country', 'SubType',
        'Document Type', 'ref: subject fields', 'citedby-count', 'issn',  'Y2013', 'CO', 'NC'
    ]
}

FIELDS = [
    'citation_count',
    'citations_overview'
]

current_key_index = 0

HEADERS = {
    'X-ELS-APIKey': API_KEY[current_key_index],
    'Accept': 'application/json'
}

search_year = None
# only_search = False

logging.basicConfig(filename='err.log', level=logging.DEBUG)


class ScopusAPI(object):
    def __init__(self, filename):
        self.base_url = BASE_URL
        self.csv_file = open(filename, 'r')
        self.reader = csv.reader(self.csv_file)
        result_file_name = filename.split('.')[0] + '-result.csv'
        self.result_file = open(result_file_name, 'w')
        self.headers = HEADERS
        self.req = urllib2.Request(self.base_url, headers=self.headers)
        self.title = []

    def get_res(self, url):
        req = urllib2.Request(url=url, headers=self.headers)
        res = urllib2.urlopen(req)
        return res

    def get_resp(self, url):
        count = 0
        while count < 3:
            count += 1
            try:
                res = self.get_res(url)
                if res.code == 200:
                    try:
                        return json.loads(res.read())
                    except httplib.IncompleteRead as e:
                        logging.error(traceback.format_exc())
                        continue
            except urllib2.HTTPError as e:
                logging.error(traceback.format_exc())
                print e
                global current_key_index
                self.headers['X-ELS-APIKey'] = API_KEY[current_key_index + 1]

        # try:
        #     res = self.get_res(url)
        #     if res.code == 200:
        #         try:
        #             data = res.read()
        #         except httplib.IncompleteRead as e:
        #             data = e.partial
        #         return json.loads(data)
        # except urllib2.HTTPError as e:
        #     logging.error(traceback.format_exc())
        #     print e
        #     global current_key_index
        #     self.headers['X-ELS-APIKey'] = API_KEY[current_key_index + 1]
        #     res = self.get_res(url)
        #     if res.code == 200:
        #         return json.loads(res.read())
        return None

    def load_data(self):
        for line in self.reader:
            if self.reader.line_num == 1:
                self.title = line
                continue
            yield line

    @classmethod
    def get_sub_areas(cls, data):
        subareas = data.get('subject-areas', {}) or {}
        subareas = subareas.get('subject-area', [])
        return list(set([sub.get('@abbrev') for sub in subareas]))

    def get_ref_list_by_eid(self, resp_data):
        def get_id(item):
            d = item.get('ref-info', {}).get('refd-itemidlist', {}).get('itemid', {}).get('$')
            return d
        data = resp_data.get('abstracts-retrieval-response', {})
        reference = data.get('item', {}).get('bibrecord', {}).get('tail', {})\
            .get('bibliography', {}).get('reference', [])
        ref_ids = [get_id(item) for item in reference]
        sub_fields = self.get_sub_areas(data)
        return {'sub_fields': sub_fields, 'ref_ids': ref_ids}

    def get_ref_ids_by_eid(self, eid):
        url = self.get_url(eid, 'ref_id')
        resp = self.get_resp(url)
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
        url = self.get_url(scopus_id, 'scopus')
        return self.get_resp(url)

    def get_aggregate(self, references):
        global search_year
        s_year = search_year or 2013
        year_count = [ref for ref in references if ref.get('year') == s_year and ref.get('affiliation country')]
        co = len([ref for ref in year_count if ref.get('affiliation country').lower() == 'china'])
        nc = len([ref for ref in year_count if 'china' not in ref.get('affiliation country').lower()])
        return {
            'y2013': len(year_count),
            'co': co,
            'nc': nc
        }

    def get_ref_info_for_export(self, rid, res_list, ref_info_list):
        ref_info = {}
        res = []
        try:
            ref_info = self.get_ref_info(rid)
            res = [''] * 21
            res.extend([
                ref_info.get('title') or ref_info.get('publicationName'),
                ref_info.get('source-id'),
                ref_info.get('eid'),
                str(ref_info.get('year')),
                ref_info.get('affiliation country'),
                ref_info.get('subtypeDescription'),
                ref_info.get('aggregationType'),
                ref_info.get('subject arears'),
                ref_info.get('citedby-count'),
                ref_info.get('issn'),
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
        ref_ids = main_data.get('ref_ids')
        sub_fields = main_data.get('sub_fields')
        src_data.append(','.join(sub_fields))
        res_list = []
        ref_agg_list = []
        thread_list = []
        for rid in ref_ids:
            try:
                t = Thread(target=self.get_ref_info_for_export, args=(rid, res_list, ref_agg_list))
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
        agg_append.extend([
            ref_agg.get('y2013'),
            ref_agg.get('co'),
            ref_agg.get('nc'),
        ])
        src_data.extend(agg_append)
        try:
            f_writer.writerow(src_data)
            f_writer.writerows(res_list)
        except Exception as e:
            traceback.print_exc()
            logging.info('[%s]: %s' % (datetime.datetime.now(), src_data))
            logging.error(traceback.format_exc())

    def get_url(self, id, data_type):
        url = '/'.join([self.base_url, URL_MAPPING.get(data_type), id])
        return url

    def get_scopus_id_by_eid(self, eid):
        li = eid.split('0-')
        scopus_id = li[-1] if len(li) == 2 else ''
        return scopus_id

    def get_serial_title(self, query_field, line):
        query = line[QUERY_FIELDS_INDEX.get(query_field)]
        url = '/'.join([self.base_url, URL_MAPPING['serial_title']]) + '?' + 'issn=' + query
        req = urllib2.Request(url=url, headers=self.headers)
        res = urllib2.urlopen(req)
        data = ''
        if res.code == 200:
            resp = json.loads(res.read())
            data = resp.get('serial-metadata-response', {}).get('entry')
        return data

    def run(self, data_type, start_num=1, end_num=None):
        ori_data = self.load_data()
        with self.result_file as f:
            f.write(codecs.BOM_UTF8)
            writer = csv.writer(f)
            w_title = False
            count = 0
            for line in ori_data:
                if not w_title:
                    self.title.extend(TITLE_MAPPING[data_type])
                    writer.writerow(self.title)
                    w_title = True
                count += 1
                if start_num > 1:
                    if count < start_num:
                        continue
                if end_num:
                    if count > end_num:
                        break
                if data_type == 'references':
                    self.get_ref(line, writer)
                if data_type == 'serial_title':
                    self.get_serial_title('issn', line)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, help='file name')
    parser.add_argument('--start', type=int, help='start line')
    parser.add_argument('--end', type=int, help='end line')
    parser.add_argument('--year', type=int, help='count year')
    args = parser.parse_args()
    src_file = args.file
    start = args.start
    end = args.end
    input_year = args.year
    if not src_file:
        print 'please input filename'
    else:
        global search_year
        search_year = int(input_year or 2013)

        query_type = 'references'
        sapi = ScopusAPI(src_file)
        sapi.run(query_type, start, end)
