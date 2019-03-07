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

# OPUS code : folder name, iso, wals, glotto
lang_dic = {
    # 'eu': ['Basque (eus)', 'eus', 'bsq', 'basq1248'],
    # 'en': ['English (eng)', 'eng', 'eng', 'stan1293'],
    # 'fi': ['Finnish (fin)', 'fin', 'fin', 'finn1318'],
    # 'fr': ['French (fra)', 'fra', 'fre', 'stan1290'],
    # 'ka': ['Georgian (kat)', 'kat', 'geo', 'nucl1302'],
    # 'de': ['German (deu)', 'deu', 'ger', 'stan1295'],
    # 'el': ['Greek_Modern (ell)', 'ell', 'grk', 'mode1248'],
    # 'he': ['Hebrew_Modern (heb)', 'heb', 'heb', 'hebr1245'],
    # 'hi': ['Hindi (hin)', 'hin', 'hin', 'hind1269'],
    # 'id': ['Indonesian (ind)', 'ind', 'ind', 'indo1316'],
    # 'ja': ['Japanese (jpn)', 'jpn', 'jpn', 'nucl1643'],
    # 'ko': ['Korean (kor)', 'kor', 'kor', 'kore1280'],
    # 'zh': ['Mandarin (cmn)', 'cmn', 'mnd', 'mand1415'],
    # 'fa': ['Persian (pes)', 'pes', 'prs', 'west2369'],
    # 'ru': ['Russian (rus)', 'rus', 'rus', 'russ1263'],
    # 'es': ['Spanish (spa)', 'spa', 'spa', 'stan1288'],
    # 'sw': ['Swahili (swh)', 'swh', 'swa', 'swah1253'],
    'tl': ['Tagalog_tgl', 'tgl', 'tag', 'taga1270']
    # 'th': ['Thai (tha)', 'tha', 'tha', 'thai1261'],
    # 'tr': ['Turkish (tur)', 'tur', 'tur', 'nucl1301'],
    # 'vi': ['Vietnamese (vie)', 'vie', 'vie', 'viet1252'],
}

# Root path
def get_root(lang):
    return '../data/' + lang_dic[lang][0] + '/non-fiction/written/'

# Request HTML page for the current language
def request(language):
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True
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
def search_xmls(lang):
    path = get_root(lang) + 'OpenSubtitles/raw/' + lang
    current_counter = 0
    for root, dirs, files in os.walk(path):
        for dirname in sorted(dirs, reverse=True):
            for files in os.walk(os.path.join(root, dirname)):
                if len(files[2]) > 0:
                    for file in files[2]:
                        text = get_text(os.path.join(files[0],file))
                        sample(text, lang, current_counter)

# Retrieve text
def get_text(file):
        f = codecs.open(file, 'r', 'utf-8')
        xml = f.read().encode('utf-8')
        parser = etree.XMLParser(encoding='utf-8')
        tree = etree.XML(xml, parser)
        text = ''
        for el in tree:
            if el.tag == 's':
                try:
                    line = etree.tostring(el, method='text')
                    text += line.decode('utf-8').strip()
                except:
                    pass
                text += '\n'
        return text

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
    fname = lang_dic[lang][1] + '_nfi_' + str(int(current_counter)+1) + '.txt'
    print(get_root(lang) + fname)
    return get_root(lang) + fname

# Tokenization
def count_tokens(text):
    result = text.lower()
    result = re.sub('[^\w ]+', ' ', result)
    result = re.sub('(  )+', ' ', result)
    tokens = result.split()
    print('Tokens: ', len(tokens))
    return len(tokens)

# Random starting point
def starting_point(text):
    pass

# Sample
def sample(text, lang, current_counter):
    fname = generate_fname(lang, current_counter)
    num_tokens = count_tokens(text)
    if num_tokens < 50000:
        f1 = codecs.open(fname, 'w', 'utf-8')
        f1.write(text)
    else:
        pass

def main():
    for lang in lang_dic:
        # print(lang)
        #
        # iso = lang_dic[lang][1]
        # genre = 'nfi'
        # name_prefix = iso + '_' + genre + '_'
        #
        # html = request(lang)
        # link = find_link(lang, html)
        # print(link)
        #
        # fname_zip = get_root(lang) + 'subs.zip'
        # get_file(link, fname_zip)
        # print('Finished downloading the zip file')
        #
        # unzip_file(fname_zip, get_root(lang))
        search_xmls(lang)




if __name__ == "__main__":
    main()
