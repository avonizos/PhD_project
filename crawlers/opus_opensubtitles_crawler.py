# Crawler for OPUS OpenSubtitles
# Gathers *.zip files, samples from them, stores *.txt and *.xml
# Total per language approx. 50 000 tokens

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
    'tl': ['Tagalog (tgl)', 'tgl', 'tag', 'taga1270']
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
    print(html)
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
    print(fname_zip)
    print(root)
    zip_ref = zipfile.ZipFile(fname_zip, 'r')
    zip_ref.extractall(root)
    zip_ref.close()
    os.remove(fname_zip)

# Count tokens
def count_tokens(lang):
    path = get_root(lang) + 'OpenSubtitles/raw/' + lang
    for root, dirs, files in os.walk(path):
        for dirname in sorted(dirs):
            print(dirname)


# Sampling
def sampling():
    pass


def main():
    for lang in lang_dic:
        print(lang)

        iso = lang_dic[lang][1]
        genre = 'nfi'
        name_prefix = iso + '_' + genre + '_'

        html = request(lang)
        link = find_link(lang, html)
        print(link)

        fname_zip = get_root(lang) + 'subs.zip'

        get_file(link, fname_zip)

        print('Finished downloading the zip file')

        unzip_file(fname_zip, get_root(lang))

        count_tokens(lang)



if __name__ == "__main__":
    main()