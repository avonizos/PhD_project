# Crawler for the files in OPUS
# Collects tar.gz and unpacks the xml files into each language folder

import requests, re, os
import urllib3
import gzip
import shutil
import tarfile
import ssl
import time
from requests.exceptions import Timeout, ConnectionError
from urllib3.exceptions import ReadTimeoutError

# Quechua languages, qu (?)
# Songhai languages, son (?)

# 2x Basque, Berber, Burmese, Chamorro, 2x English,
# 2x French, Georgian, 2x German, Greek, Greenlandic (Kalaallisut),
# Guarani, Hausa, Hebrew, 2x Hindi, Indonesian, 2x Japanese,
# Kannada, Khalkha (Mongolian), Korean, Malagasy,
# 4x Mandarin, Mapudungun, Oromo, 2x Persian, Russian,
# 2x Spanish, Swahili, Tagalog, Thai,
# Turkish, Vietnamese, Yoruba, Zulu


lang_dic = {
    'eu_ES': 'Basque',
    'eu': 'Basque',
    'ber': 'Berber',
    'my': 'Burmese',
    'ch': 'Chamorro',
    'en': 'English',
    'en_GB': 'English',
    'fr': 'French',
    'fr_FR': 'French',
    'ka': 'Georgian',
    'de': 'German',
    'de_DE': 'German',
    'el': 'Greek',
    'kl': 'Greenlandic',
    'gn': 'Guarani',
    'ha': 'Hausa',
    'he': 'Hebrew',
    'hi': 'Hindi',
    'hi_IN': 'Hindi',
    'id': 'Indonesian',
    'jp': 'Japanese',
    'ja': 'Japanese',
    'kn': 'Kannada',
    'mn': 'Khalkha',
    'ko': 'Korean',
    'mg': 'Malagasy',
    'zh': 'Mandarin',
    'zh_zh': 'Mandarin',
    'zh_cn': 'Mandarin',
    'zh_CN': 'Mandarin',
    'arn': 'Mapudungun',
    'om': 'Oromo',
    'fa': 'Persian',
    'fa_IR': 'Persian',
    'ru': 'Russian',
    'es': 'Spanish',
    'es_ES': 'Spanish',
    'sw': 'Swahili',
    'tl': 'Tagalog',
    'tl_PH': 'Tagalog',
    'th': 'Thai',
    'tr': 'Turkish',
    'tr_TR': 'Turkish',
    'vi': 'Vietnamese',
    'vi_VN': 'Vietnamese',
    'yo': 'Yoruba',
    'zu': 'Zulu'
}

# Request HTML page for the current language
def request(language):
    r = requests.post("http://opus.nlpl.eu/", data={'src': language, 'trg': 'any'})
    html = r.text
    return html

# Find all links to the monolingual raw *.tar.gz files
def find_links(language, html):
    find_link = re.compile('<a href="(download\.php\?f=[a-zA-Z0-9\-]+%2F' + language + '\.raw\.tar\.gz)"')
    links = re.findall(find_link, html)
    return links

# Return links as a list
def list_links(links):
    result = []
    if len(links) > 0:
        result = [link for link in links]
    return result

# Create folders for each language
def create_dir(lang):
    root = '../OPUS/'
    root_path = '../OPUS/' + lang_dic[lang]
    embedded_path = '../OPUS/' + lang_dic[lang] + '/' + lang
    if not os.path.isdir(root):
        os.mkdir(root)
    if not os.path.isdir(root_path):
        os.mkdir(root_path)
    if not os.path.isdir(embedded_path):
        os.mkdir(embedded_path)

# Define filenames to save
def define_fname(lang, url, counter):
    dir = '../OPUS/' + lang_dic[lang] + '/' + lang
    find_name = re.search('f=(.*)%2F(' + lang + '\.raw\.tar\.gz)', url)
    if find_name is not None:
        fname = dir + '/' + str(counter) + '_' + find_name.group(2)
        file_cat = find_name.group(1)
        print(fname)
        return fname, file_cat

# Get request to obtain *.tar.gz files
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

# Untar *.tar.gz files
def untar(fname, lang, counter):
    tar = tarfile.open(fname)
    with tarfile.open(fname) as tar:
        path = '../OPUS/' + lang_dic[lang] + '/' + lang + '/' + str(counter)
        if not os.path.isdir(path):
            os.mkdir(path)
            tar.extractall(path=path)
    os.remove(fname)

# Gunzip *.xml.gz files
def gunzip(file_cat, lang, counter):
    initial_path = '../OPUS/' + lang_dic[lang] + '/' + lang
    for root, dirs, files in os.walk(initial_path + '/' + str(counter)):
        for name in files:
            if name.endswith('gz'):
                filename = os.path.join(root,name)
                with gzip.open(filename, 'rb') as f_in:
                    if not os.path.isdir(initial_path + '/' + file_cat):
                        os.mkdir(initial_path + '/' + file_cat)
                    if not os.path.isdir(initial_path + '/' + file_cat + '/' + str(counter)):
                        os.mkdir(initial_path + '/' + file_cat + '/' + str(counter))
                    xml_name = initial_path + '/' + file_cat + '/' + str(counter) + '/' + str(counter) + '_' + name[:-7]
                    with open(xml_name + '.xml', 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                        print('GUNZIP FILE: ' + xml_name + '.xml')
    shutil.rmtree(initial_path + '/' + str(counter))

# Download, gunzip, place nicely into the folder
def download(lang, links):
    counter = 1
    for link in links:
        url = 'http://opus.nlpl.eu/' + link
        print('DOWNLOAD: ' + url)
        fname_cat = define_fname(lang, url, counter)
        fname = fname_cat[0]
        file_cat = fname_cat[1]
        get_file(url, fname)
        untar(fname, lang, counter)
        gunzip(file_cat, lang, counter)
        counter += 1

def main():
    for lang in lang_dic:
        html = request(lang)
        links = list_links(find_links(lang, html))
        create_dir(lang)
        download(lang, links)

if __name__ == "__main__":
    main()

