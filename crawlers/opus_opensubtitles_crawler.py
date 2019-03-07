# -*- coding: utf-8 -*-

# Crawler for OPUS OpenSubtitles
# Gathers *.zip files, samples from them, stores *.txt and *.xml
# 100 samples
# 50 000 tokens per sample
# if a text has less than 50 000 tokens -> take the whole text

import requests, re, os
import logging
import urllib3
import zipfile
import gzip
import shutil
import codecs
import tarfile
import ssl
import time
import random
from lxml import etree
from requests.exceptions import Timeout, ConnectionError
from urllib3.exceptions import ReadTimeoutError
from io import StringIO, BytesIO

#'ber': ['Berber_Middle_Atlas (tzm)', 'tzm', 'bma', 'cent2194'],
#'my': ['Burmese (mya)', 'mya', 'brm', 'nucl1310'],
#'ch': ['Chamorro (cha)', 'cha', 'cha', 'cham1312'],
#'kl': ['Greenlandic (kal)', 'kal', 'grw', 'kala1399'],
#'gn': ['Guarani (gug)', 'gug', 'gua', 'para1311'],
#'ha': ['Hausa (hau)', 'hau', 'hau', 'haus1257'],
#'kn': ['Kannada (kan)', 'kan', 'knd', 'nucl1305'],
#'mn': ['Khalkha (khk)', 'khk', 'kha', 'halh1238'],
#'mg': ['Malagasy (plt)', 'plt', 'mal', 'plat1254'],
#'arn': ['Mapudungun (arn)', 'arn', 'map', 'mapu1245'],
#'om': ['Oromo_Harar (hae)', 'hae', 'orh', 'east2652'],
#'yo': ['Yoruba (yor)', 'yor', 'yor', 'yoru1245'],
#'zu': ['Zulu (zul)', 'zul', 'zul', 'zulu1248']

# OPUS code : folder name, iso, name_wals, name_glotto
lang_dic = {
    'eu': ['Basque_eus', 'eus', 'Basque', 'Basque', 'Latn'],
    'en': ['English_eng', 'English', 'English', 'Latn'],
    'fi': ['Finnish_fin', 'Finnish', 'Finnish', 'Latn'],
    'fr': ['French_fra', 'French', 'French', 'Latn'],
    'ka': ['Georgian_kat', 'Georgian', 'Georgian', 'Geor'],
    'de': ['German_deu', 'German', 'German', 'Latn'],
    'el': ['Greek_Modern_ell', 'Greek (Modern)', 'Modern Greek', 'Grek'],
    'he': ['Hebrew_Modern_heb', 'Hebrew (Modern)', 'Modern Hebrew', 'Hebr'],
    'hi': ['Hindi_hin', 'Hindi', 'Hindi', 'Deva'],
    'id': ['Indonesian_ind', 'Indonesian', 'Indonesian', 'Latn'],
    'ja': ['Japanese_jpn', 'Japanese', 'Japanese', 'Jpan'],
    'ko': ['Korean_kor', 'Korean', 'Korean', 'Kore'],
    'zh': ['Mandarin_cmn', 'Mandarin', 'Mandarin Chinese', 'Hans'],
    'fa': ['Persian_pes', 'Persian', 'Western Farsi', 'pes'],
    'ru': ['Russian_rus', 'Russian', 'Russian', 'Cyrl'],
    'es': ['Spanish (spa)', 'spa', 'spa', 'stan1288'],
    'sw': ['Swahili (swh)', 'swh', 'swa', 'swah1253'],
    'tl': ['Tagalog_tgl', 'tgl', 'Tagalog', 'Tagalog', 'Latn']
    # 'th': ['Thai (tha)', 'tha', 'tha', 'thai1261'],
    # 'tr': ['Turkish (tur)', 'tur', 'tur', 'nucl1301'],
    # 'vi': ['Vietnamese (vie)', 'vie', 'vie', 'viet1252'],
}

# Root path
def get_root(lang):
    return '../data/' + lang_dic[lang][0] + '/non-fiction/written/'

# Request HTML page for the current language
def request(language):
    # logging.basicConfig()
    # logging.getLogger().setLevel(logging.DEBUG)
    # requests_log = logging.getLogger("requests.packages.urllib3")
    # requests_log.setLevel(logging.DEBUG)
    # requests_log.propagate = True
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    r = requests.post('http://opus.nlpl.eu/index.php', data={'src': language, 'trg': 'any', 'minsize': 'all'})
    html = r.text
    print('HTML loaded')
    return html

# Find link to the *.zip with OpenSubtitles, the newest version possible
def find_link(language, html):
    find_link = re.compile('https://object.pouta.csc.fi/OPUS-OpenSubtitles/v201[86321]/raw/' + language + '.zip')
    link = re.search(find_link, html)
    print(link.group(0))
    if link is None:
        return ('No subtitles for the language ' + language)
    else:
        return link.group(0)

# Get request to obtain *.zip files
def get_file(url, fname):
    try:
        http = urllib3.PoolManager()
    except (Timeout, ssl.SSLError, ReadTimeoutError, ConnectionError) as exc:
        time.sleep(30)
        http = urllib3.PoolManager()
    r = http.request('GET', url, preload_content=False, headers={'User-Agent': 'Mozilla/5.0'})
    CHUNK = 16 * 1024
    with open(fname, 'wb') as fp:
        while True:
            chunk = r.read(CHUNK)
            if not chunk: break
            fp.write(chunk)
    r.release_conn()

# Unzip file
def unzip_file(fname_zip, root):
    zip_ref = zipfile.ZipFile(fname_zip, 'r')
    zip_ref.extractall(root)
    zip_ref.close()
    os.remove(fname_zip)

# Search xmls
def search_xmls(lang, link):
    path = get_root(lang) + 'OpenSubtitles/raw/' + lang
    current_counter = 0
    for root, dirs, files in os.walk(path):
        for dirname in sorted(dirs, reverse=True):
            for files in os.walk(os.path.join(root, dirname)):
                if len(files[2]) > 0:
                    for file in files[2]:
                        text, year, tokens = get_text(os.path.join(files[0],file))
                        sample(link, text, year, tokens, lang, current_counter)

# Remove initials xmls
def remove_xmls(lang):
    path = get_root(lang) + 'OpenSubtitles'
    shutil.rmtree(path)
    os.remove(get_root(lang) + 'INFO')
    os.remove(get_root(lang) + 'LICENSE')
    os.remove(get_root(lang) + 'README')


# Retrieve text
def get_text(file):
    f = codecs.open(file, 'r', 'utf-8')
    xml = f.read().encode('utf-8')
    parser = etree.XMLParser(encoding='utf-8')
    tree = etree.XML(xml, parser)
    text = ''
    year = 'NA'
    tokens = 0
    for el in tree:
        if el.tag == 's':
            try:
                line = etree.tostring(el, method='text')
                text += line.decode('utf-8').strip()
            except:
                pass
            text += '\n'

        if el.tag == 'meta':
            for child in el:
                if child.tag == 'source':
                    for c in child:
                        if c.tag == 'year':
                            year = c.text
                            print(year)

                if child.tag == 'conversion':
                    for c in child:
                        if c.tag == 'tokens':
                            tokens = c.text
                            print(tokens)

    return text, year, tokens

# Generate file name
def generate_fname(lang, current_counter):
    path = get_root(lang)
    max_counter = 0
    search_fcounter = re.compile(lang_dic[lang][1] + '_nfi_' + '([0-9]+)(\.txt)?')
    for root, dirs, files in os.walk(path):
        for file in files:
            fcounter = re.search(search_fcounter, file)
            if fcounter is not None:
                if int(fcounter.group(1)) > max_counter:
                    current_counter = fcounter.group(1)
                    max_counter = int(current_counter)
            else:
                current_counter = 0
    if max_counter > 0:
        current_counter = max_counter
    fname = lang_dic[lang][1] + '_nfi_' + str(int(current_counter)+1) + '.txt'
    print(get_root(lang) + fname)
    return get_root(lang) + fname

# Tokenization
def count_tokens(text):
    pass
    # result = text.lower()
    # result = re.sub('[^\w ]+', ' ', result)
    # result = re.sub('(  )+', ' ', result)
    # tokens = result.split()
    # print('Tokens: ', len(tokens))
    # return len(tokens)

# Random starting point
def starting_point(text):
    pass

# Sample
def sample(link, text, year, tokens, lang, current_counter):
    fname = generate_fname(lang, current_counter)
    if int(tokens) < 50000:
        f1 = codecs.open(fname, 'w', 'utf-8')
        meta = '''# language_name_wals:	''' + lang_dic[lang][2] + '''
# language_name_glotto:	''' + lang_dic[lang][3] + '''
# ISO_639-3:	''' + lang_dic[lang][1] + '''
# year_composed:	NA
# year_published:	''' + year + '''
# mode:	written
# genre_(broad):	non-fiction
# genre_(narrow):	prepared_speeches
# writing_system:	''' + lang_dic[lang][4] + '''
# special_characters:	NA
# short_description:	OpenSubtitles2018
# source:	''' + link + '''
# copyright_short:	http://www.opensubtitles.org/
# copyright_long:	http://www.opensubtitles.org/ P. Lison and J. Tiedemann, 2016, OpenSubtitles2016: Extracting Large Parallel Corpora from Movie and TV Subtitles. In Proceedings of the 10th International Conference on Language Resources and Evaluation (LREC 2016)
# sample_type:	whole
# comments:	NA

'''
        f1.write(meta)
        f1.write(text[:-1])
    else:
        pass

def main():
    for lang in lang_dic:
        print(lang_dic[lang][2])

        html = request(lang)
        link = find_link(lang, html)
        print(link)

        fname_zip = get_root(lang) + 'subs.zip'
        get_file(link, fname_zip)
        print('Finished downloading the zip file')

        unzip_file(fname_zip, get_root(lang))
        search_xmls(lang, link)

        remove_xmls(lang)



if __name__ == "__main__":
    main()
