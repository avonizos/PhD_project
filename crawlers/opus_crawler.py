# Crawler for the files in OPUS
# Collects tar.gz and unpacks the xml files into each language folder

import requests, re, os
import urllib3
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

amount_tokens = 50000

# Quechua languages, qu (?)
# Songhai languages, son (?)

# 2x Basque, Berber, Burmese, Chamorro, 2x English,
# 2x French, Georgian, 2x German, Greek, Greenlandic (Kalaallisut),
# Guarani, Hausa, Hebrew, 2x Hindi, Indonesian, 2x Japanese,
# Kannada, Khalkha (Mongolian), Korean, Malagasy,
# 4x Mandarin, Mapudungun, Oromo, 2x Persian, Russian,
# 2x Spanish, Swahili, Tagalog, Thai,
# Turkish, Vietnamese, Yoruba, Zulu

prefix = '../data/OPUS/'

lang_dic = {
#     'eu_ES': 'Basque (eus)',
#     'eu': 'Basque (eus)',
#     'ber': 'Berber (tzm)',
#     'my': 'Burmese (mya)',
     'ch': 'Chamorro (cha)',
#     'en': 'English (eng)',
#     'en_GB': 'English (eng)',
#     'fr': 'French (fra)',
#     'fr_FR': 'French (fra)',
#     'ka': 'Georgian (kat)',
#     'de': 'German (deu)',
#     'de_DE': 'German (deu)',
#     'el': 'Greek (ell)',
#     'kl': 'Greenlandic (kal)',
#     'gn': 'Guarani (gug)',
#     'ha': 'Hausa (hau)',
#     'he': 'Hebrew (heb)',
#     'hi': 'Hindi (hin)',
#     'hi_IN': 'Hindi (hin)',
#     'id': 'Indonesian (ind)',
#     'jp': 'Japanese (jpn)',
#     'ja': 'Japanese (jpn)',
#     'kn': 'Kannada (kan)',
#     'mn': 'Khalkha (khk)',
#     'ko': 'Korean (kor)',
#     'mg': 'Malagasy (plt)',
#     'zh': 'Mandarin (cmn)',
#     'zh_zh': 'Mandarin (cmn)',
#     'zh_cn': 'Mandarin (cmn)',
#     'zh_CN': 'Mandarin (cmn)',
#     'arn': 'Mapudungun (arn)',
#     'om': 'Oromo (hae)',
#     'fa': 'Persian (pes)',
#     'fa_IR': 'Persian (pes)',
#     'ru': 'Russian (rus)',
#     'es': 'Spanish (spa)',
#     'es_ES': 'Spanish (spa)',
#     'sw': 'Swahili (swh)',
#     'tl': 'Tagalog (tgl)',
#     'tl_PH': 'Tagalog (tgl)',
#     'th': 'Thai (tha)',
#     'tr': 'Turkish (tur)',
#     'tr_TR': 'Turkish (tur)',
#     'vi': 'Vietnamese (vie)',
#     'vi_VN': 'Vietnamese (vie)',
#     'yo': 'Yoruba (yor)',
#     'zu': 'Zulu (zul)'
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
    root = prefix
    root_path = prefix + lang_dic[lang]
    embedded_path = prefix + lang_dic[lang] + '/' + lang
    if not os.path.isdir(root):
        os.mkdir(root)
    if not os.path.isdir(root_path):
        os.mkdir(root_path)
    if not os.path.isdir(embedded_path):
        os.mkdir(embedded_path)

# Define filenames to save
def define_fname(lang, url, counter):
    dir = prefix + lang_dic[lang] + '/' + lang
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
        path = prefix + lang_dic[lang] + '/' + lang + '/' + str(counter)
        if not os.path.isdir(path):
            os.mkdir(path)
            tar.extractall(path=path)
    os.remove(fname)

# Gunzip *.xml.gz files
def gunzip(file_cat, lang, counter):
    initial_path = prefix + lang_dic[lang] + '/' + lang
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

# How many XML files are there for this language
def count_amount(lang):
    path = prefix + lang_dic[lang] + '/' + lang
    amount_files = 0
    amount_dirs = 0
    list_dirs = []
    list_files = []
    numbers = re.compile('^[0-9]+$')
    for root, dirs, files in os.walk(path):
        for dir in dirs:
            number_name = re.search(numbers, dir)
            if number_name is not None:
                amount_dirs += 1
                list_dirs.append(os.path.join(root, dir))
        for name in files:
            if name.endswith('xml'):
                amount_files += 1
                list_files.append(os.path.join(root, name))
    per_dir = amount_tokens / amount_dirs
    per_file = amount_tokens / amount_files
    print('TOKENS PER DIR: %d' % per_dir)
    print('TOKENS PER FILE: %d' % per_file)
    return per_dir, per_file, list_dirs, list_files

# Parse text from XML
def parse_xml(xml):
    root = etree.fromstring(xml)
    sentences = []
    amount_tokens = 0
    for el in root.iter():
        current_tokens = el.text.lower().split()
        sentences.append(current_tokens)
        amount_tokens += len(current_tokens)
    return sentences, amount_tokens

def parse(lang, per_dir, per_file, list_dirs, list_files):

    tokens_files = 0
    used_dirs = set([])
    used_files = set([])
    path = prefix + lang_dic[lang] + '/' + lang


    while tokens_files <= amount_tokens and len(list_files) != len(used_files):

        tokens_dirs = 0
        while tokens_dirs <= per_dir and len(list_dirs) != len(used_dirs):
            random_dir = random.choice(list_dirs)
            current_files = [f for f in os.listdir(random_dir) if os.path.isfile(os.path.join(random_dir, f))]
            for file in current_files:
                print(file)
                current_file = os.path.join(random_dir, file)
                with open(current_file, 'rb') as f:
                    xml = f.read()
                    parsed = parse_xml(xml)

                    if parsed[1] <= per_file:
                        result = parsed[0]

                    tokens_dirs += parsed[1]
                    tokens_files += parsed[1]

                used_files.add(file)

            used_dirs.add(random_dir)


    print('TOKENS GATHERED: %d' % tokens_files)



# Download, gunzip, place nicely into the folder
def download(lang, links):
    counter = 1
    for link in links:
        url = 'http://opus.nlpl.eu/' + link
        print('DOWNLOADING: ' + url)
        fname_cat = define_fname(lang, url, counter)
        fname = fname_cat[0]
        file_cat = fname_cat[1]
        get_file(url, fname)
        untar(fname, lang, counter)
        gunzip(file_cat, lang, counter)
        counter += 1

    amount = count_amount(lang)
    per_dir = amount[0]
    per_file = amount[1]
    list_dirs = amount[2]
    list_files = amount[3]

    parse(lang, per_dir, per_file, list_dirs, list_files)


def main():
    for lang in lang_dic:
        html = request(lang)
        links = list_links(find_links(lang, html))
        create_dir(lang)
        download(lang, links)

if __name__ == "__main__":
    main()
