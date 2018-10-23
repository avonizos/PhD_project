# Crawler for the Wikipedia Dumps
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

import requests

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
def request():
    r = requests.post("https://dumps.wikimedia.your.org/backup-index.html")
    html = r.text
    return html

# Find all links to the *.tar.gz files for a certain language
def find_links(language, html):
    find_link = re.compile('<a href="(' + language + 'wik.*?)"')
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
    root = './Wiki/'
    root_path = './Wiki/' + lang_dic[lang]
    embedded_path = './Wiki/' + lang_dic[lang] + '/' + lang
    if not os.path.isdir(root):
        os.mkdir(root)
    if not os.path.isdir(root_path):
        os.mkdir(root_path)
    if not os.path.isdir(embedded_path):
        os.mkdir(embedded_path)

def main():
    html = request()
    for lang in lang_dic:
        links = list_links(find_links(lang, html))
        print lang_dic[lang] + ':'
        print links

if __name__ == "__main__":
    main()
