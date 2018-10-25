# Crawler for the Wikipedia Dumps
# Collects bz2 files and unpacks the xml files into each language folder

import requests, re, os
import urllib3
import bz2
import ssl
import time
from requests.exceptions import Timeout, ConnectionError
from urllib3.exceptions import ReadTimeoutError
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



# qu -- Quechua?

lang_dic = {
"ab": "Abkhaz",
"arz": "Arabic (Egyptian)",
"eu": "Basque",
"my": "Burmese",
"ch": "Chamorro",
"cr": "Cree",
"en": "English",
"simple": "Simple English",
"fj": "Fijian",
"fi": "Finnish",
"fr": "French",
"ka": "Georgian",
"de": "German",
"el": "Greek",
"kl": "Greenlandic",
"gn": "Guarani",
"ha": "Hausa",
"he": "Hebrew",
"hi": "Hindi",
"id": "Indonesian",
"ja": "Japanese",
"kn": "Kannada",
"mn": "Khalkha",
"ko": "Korean",
"lez": "Lezgian",
"mg": "Malagasy",
"zh": "Mandarin",
"bpy": "Meithei",
"om": "Oromo",
"fa": "Persian",
"ru": "Russian",
"sg": "Sango",
"es": "Spanish",
"sw": "Swahili",
"tl": "Tagalog",
"th": "Thai",
"tr": "Turkish",
"vi": "Vietnamese",
"yo": "Yoruba",
"zu": "Zulu"
}

# Request HTML page for the current language
def request(link):
    r = requests.post(link)
    html = r.text
    return html

# Find all links to the *.tar.gz files for a certain language
def find_links(language, html):
    file_links = []
    find_link = re.compile('<a href="(' + language + 'wik.*?)"')
    find_file = re.compile('<a href="(.*?pages-articles.xml.bz2)">')
    links = re.findall(find_link, html)
    for link in links:
        html = request("https://dumps.wikimedia.your.org/" + link)
        file = re.search(find_file, html)
        if file is not None:
            file_links.append(file.group(1))
    return file_links

# Return links as a list
def list_links(links):
    result = []
    if len(links) > 0:
        result = [link for link in links]
    return result

# Create folders for each language
def create_dir(lang):
    root = './Wiki/'
    root_path = './Wiki/' + lang_dic[lang]
    embedded_path = './Wiki/' + lang_dic[lang] + '/' + lang
    if not os.path.isdir(root):
        os.mkdir(root)
    if not os.path.isdir(root_path):
        os.mkdir(root_path)
    if not os.path.isdir(embedded_path):
        os.mkdir(embedded_path)

# Define filenames to save
def define_fname(lang, url, counter):
    dir = './Wiki/' + lang_dic[lang] + '/' + lang
    find_name = re.search('.*/(.*pages-articles.xml.bz2)', url)
    if find_name is not None:
        fname = dir + '/' + find_name.group(1)
        return fname

# Get request to obtain *.bz2 files
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
    size = os.path.getsize(fname)
    if float(size)/1000000000 < 1:
        result_size = str(round(float(size)/1000000, 2)) + ' MB'
    else:
        result_size = str(round(float(size) /1000000000, 2)) + ' GB'

    print('*.BZ2 FILE SIZE: %s' % result_size)

# Decompress *.bz2
def decompress_bz2(filename):
    bzfile = bz2.BZ2File(filename)
    data = bzfile.read()
    newfilepath = filename[:-4]  # assuming the filepath ends with .bz2
    open(newfilepath, 'wb').write(data)
    return newfilepath

# The function gets the filename of xml file as input and creates smallers chunks of it in the diretory
def split_xml(filename, newfilepath, lang, counter):

    # Check and create chunk directory
    dir = './Wiki/' + lang_dic[lang] + '/' + lang + '/' + str(counter)
    if not os.path.isdir(dir):
        os.mkdir(dir)
    find_name = re.search('.*/(.*pages-articles.xml)', newfilepath)


    # Counters
    pagecount = 0
    filecount = 1

    #open chunkfile in write mode
    chunkname = lambda filecount: os.path.join(dir,"chunk_"+str(filecount)+ "_" + find_name.group(1))
    chunkfile = open(chunkname(filecount), 'w')

    xmlfile = open(newfilepath, "r")

    for line in xmlfile:
        chunkfile.write(line)
        # the </page> determines new wiki page
        if '</page>' in line:
            pagecount += 1
        if pagecount > 1999:
            #print chunkname() # For Debugging
            chunkfile.close()
            pagecount = 0 # RESET pagecount
            filecount += 1 # increment filename
            chunkfile = open(chunkname(filecount), 'w')
            print('CHUNK %d COMPLETE' % (filecount-1))
    try:
        chunkfile.close()
    except:
        print 'Files already close'

    os.remove(filename)
    os.remove(newfilepath)

# Download, decompress, place nicely into the folders
def download(lang, links):
    counter = 1
    for link in links:
        url = 'https://dumps.wikimedia.your.org' + link
        print('\nLINK #%d' % counter)
        print('DOWNLOADING: ' + url)
        fname = define_fname(lang, url, counter)
        get_file(url, fname)
        print('DOWNLOADING COMPLETE\n')
        print('DECOMPRESSING: %s' % fname)
        newfilepath = decompress_bz2(fname)
        print('SPLITTING XML: %s' % newfilepath)
        split_xml(fname, newfilepath, lang, counter)
        counter += 1

def main():
    html = request("https://dumps.wikimedia.your.org/backup-index.html")
    for lang in lang_dic:
        links = list_links(find_links(lang, html))
        print lang_dic[lang].upper()
        print "AMOUNT OF FILES: %d" % len(links)
        create_dir(lang)
        download(lang, links)
        print '\n'

if __name__ == "__main__":
    main()
