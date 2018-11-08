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
#     'ch': 'Chamorro (cha)',
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
     'om': 'Oromo (hae)',
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
    print(initial_path + '/' + str(counter))
    shutil.rmtree(initial_path + '/' + str(counter), ignore_errors=True)

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
    per_dir = round(amount_tokens / amount_dirs)
    per_file = round(amount_tokens / amount_files)
    print('TOKENS PER DIR: %d' % per_dir)
    print('TOKENS PER FILE: %d' % per_file)
    return per_dir, per_file, list_dirs, list_files

# Parse text from XML
def parse_xml(xml):
    root = etree.fromstring(xml)
    sentences = []
    amount_tokens = 0
    for el in root.iter():
        if el.text is not None:
            if '\n' not in el.text:
                data = el.text.split(' ')
                sentences.append(data)
                amount_tokens += len(data)
    return sentences, amount_tokens

def parse(lang, per_dir, per_file, list_dirs, list_files):
    tokens_files = 0
    used_dirs = 0
    used_files = 0
    path = prefix + lang_dic[lang] + '/' + lang
    csv_file = codecs.open('opus_lang_tokens.csv', 'w', 'utf-8')
    csv_file.write('language;tokens\n')

    while (tokens_files <= amount_tokens and len(list_files) > used_files) and (len(list_files) > 0):
        tokens_dirs = 0
        print('TOKEN FILES:', tokens_files)
        print('LIST FILES:', len(list_files))
        print('USED FILES', used_files)
        random_dir = random.choice(list_dirs)
        flag = False
        for dirpath, dirnames, files in os.walk(random_dir):
            if files:
                flag = True
        while tokens_dirs <= per_dir and (len(list_dirs) > 0):
            if flag == True:
                print('LIST DIRS', len(list_dirs))
                print('USED DIRS', used_dirs)
                current_files = [f for f in os.listdir(random_dir) if os.path.isfile(os.path.join(random_dir, f))]
                result = []
                if len(current_files) > 0:
                    random_file = random.choice(current_files)
                    current_file = os.path.join(random_dir, random_file)
                    with open(current_file, 'rb') as f:
                        xml = f.read()
                        parsed = parse_xml(xml)
                        result += parsed[0]
                        if parsed[1] <= per_file:
                            tokens_files += parsed[1]
                            tokens_dirs += parsed[1]
                            print('CURRENT FILE: %d TOKENS, %s' % (parsed[1], current_file))
                        else:
                            counter = 0
                            while counter < per_file:
                                random_line = random.randint(0, len(parsed[0])-1)
                                result += parsed[0][random_line]
                                tokens_files += len(parsed[0][random_line])
                                tokens_dirs += len(parsed[0][random_line])
                                print('CURRENT FILE: %d TOKENS, %s' % (len(parsed[0][random_line]), current_file))
                                counter += len(parsed[0][random_line])
                    used_files += 1
                    os.remove(current_file)
                    list_files.remove(current_file)
                    parent_dir = os.path.abspath(os.path.join(random_dir, os.pardir))
                    if os.path.exists(os.path.join(parent_dir, 'sample.txt')):
                        f = codecs.open(os.path.join(parent_dir, 'sample.txt'), 'a', 'utf-8')
                    else:
                        f = codecs.open(os.path.join(parent_dir, 'sample.txt'), 'w', 'utf-8')

                    for i in range(len(result)):
                        str_line = ''
                        if isinstance(result[i], list) and len(result[i]) > 0:
                            str_line += ' '.join(result[i])
                        elif isinstance(result[i], str) and result[i] != '':
                            str_line = result[i]
                        if str_line != '\n' and str_line != '':
                            f.write(str_line)
                        if i != len(result)-1 and len(str_line) > 0:
                            f.write('\n')
                    f.close()

                    flag = False
                    for dirpath, dirnames, files in os.walk(random_dir):
                        if files:
                            flag = True
                else:
                    used_dirs += 1
                    shutil.rmtree(random_dir)
                    list_dirs.remove(random_dir)
                    random_dir = random.choice(list_dirs)
                    print(random_dir)
                    flag = False
                    for dirpath, dirnames, files in os.walk(random_dir):
                        if files:
                            flag = True
            else:
                used_dirs += 1
                shutil.rmtree(random_dir)
                list_dirs.remove(random_dir)
                random_dir = random.choice(list_dirs)
                print(random_dir)
                flag = False
                for dirpath, dirnames, files in os.walk(random_dir):
                    if files:
                        flag = True

        if tokens_dirs == 0:
            break
    print('TOKENS GATHERED: %d' % tokens_files)
    csv_file.write(lang + ';' + str(tokens_files) + '\n')

def parse_all(lang):
    amount = count_amount(lang)
    per_dir = amount[0]
    per_file = amount[1]
    list_dirs = amount[2]
    list_files = amount[3]
    parse(lang, per_dir, per_file, list_dirs, list_files)

# Download, gunzip, place nicely into the folder
def download(lang, links):
    counter = 1
    print('TOTAL ARCHIVES: %d' % len(links))
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

def main():
    for lang in lang_dic:
        html = request(lang)
        links = list_links(find_links(lang, html))
        create_dir(lang)
        download(lang, links)
        print("FINISHED COLLECTING DOCUMENTS")
        print("\nSTART SAMPLING")
        parse_all(lang)
        print('SAMPLING DONE\n')

if __name__ == "__main__":
    main()
