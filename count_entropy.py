import os
import re
import math
import codecs

path = 'UD/ud-treebanks-v2.3'

result = codecs.open('result.csv', 'w', 'utf-8')
result.write('language;filename;entropy_raw;entropy_lemmas;difference\n')

# Lowercase, delete punctuation
# Get frequency of the word types, divide it by the amount of all words
# Calculate entropy

find_word = re.compile('^[0-9]\t(.*?)\t(.*?)\t')

find_language = re.compile('UD_(.*?)-')

def read_all(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('conllu'):
                language = re.search(find_language, root)
                if language is not None:
                    lang = language.group(1)
                    entropies = unigram_entropy(os.path.join(root,file))
                    result_string = lang + ';' + file + ';' + str(entropies[0]) + ';' + str(entropies[1]) + ';' + str(entropies[0] - entropies[1]) + '\n'
                    result.write(result_string)
                    print(lang + ' ' + file + ' ' + 'in process')

def read_text(fname):
    freq_dict_raw = {}
    freq_dict_lemmas = {}
    f = codecs.open(fname, 'r', 'utf-8')
    for line in f:
        if 'PUNCT' not in line and 'SYM' not in line:
            word = re.search(find_word, line)
            if word is not None:
                raw = word.group(1).lower()
                if raw not in freq_dict_raw:
                    freq_dict_raw[raw] = 1
                else:
                    freq_dict_raw[raw] += 1

                lemma = word.group(2).lower()
                if lemma not in freq_dict_lemmas:
                    freq_dict_lemmas[lemma] = 1
                else:
                    freq_dict_lemmas[lemma] += 1

    print(freq_dict_raw)
    print(freq_dict_lemmas)

    return freq_dict_raw, freq_dict_lemmas

def unigram_entropy(fname, base = 2.0):
    freq_dicts = read_text(fname)
    freq_dict_raw = freq_dicts[0]
    freq_dict_lemmas = freq_dicts[1]

    freqs_raw = [freq_dict_raw[w] / len(freq_dict_raw) for w in freq_dict_raw]
    freqs_lemmas = [freq_dict_lemmas[w] / len(freq_dict_raw) for w in freq_dict_lemmas]

    # MLE entropy
    entropy_raw = -sum([pk * math.log(pk) / math.log(base) for pk in freqs_raw])
    entropy_lemmas = -sum([pk * math.log(pk) / math.log(base) for pk in freqs_lemmas])

    return round(entropy_raw, 3), round(entropy_lemmas, 3)

read_all(path)
