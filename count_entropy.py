import os
import re
import math
import codecs

path = 'UD/ud-treebanks-v2.3/UD_Cantonese-HK'

result = codecs.open('result.csv', 'w', 'utf-8')
result.write('language;filename;amount_tokens;amount_types_raw,amount_types_lemmas;entropy_raw;entropy_lemmas;difference\n')

# Lowercase, delete punctuation
# Get frequency of the word types, divide it by the amount of all words
# Calculate entropy


find_word = re.compile('^[0-9]\t(.*?)\t(.*?)\t')

find_language = re.compile('UD_(.*?)-')

languages = {}

def read_all(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('conllu'):
                language = re.search(find_language, root)
                if language is not None:
                    lang = language.group(1)
                    if lang not in languages:
                        languages[lang] = [file]
                    else:
                        languages[lang].append(file)
                    res = unigram_entropy(os.path.join(root,file))
                    result_string = lang + ';' + file + ';' + str(res[0]) + ';' + str(res[1]) + ';' + str(res[2]) + ';' + str(res[3]) + ';' + str(res[4]) + ';' + str(round(res[3] - res[4], 3)) + '\n'
                    result.write(result_string)
                    print(lang + ' ' + file + ' ' + 'in process')

    for key in sorted(languages, key=languages.get):
        print(key, languages[key])

    print('Amount languages:', len(languages))
    sum_f = 0
    for l in languages:
        sum_f += len(languages[l])
    print('Amount files:', sum_f)


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

    # print(freq_dict_raw)
    # print(freq_dict_lemmas)
    #
    # for key in sorted(freq_dict_lemmas, key=freq_dict_lemmas.get):
    #     if freq_dict_lemmas[key] > 20:
    #         print(key, freq_dict_lemmas[key])

    for key in freq_dict_lemmas:
        if key not in freq_dict_raw:
            print(key   )

    return freq_dict_raw, freq_dict_lemmas

def unigram_entropy(fname, base = 2.0):
    freq_dicts = read_text(fname)
    freq_dict_raw = freq_dicts[0]
    freq_dict_lemmas = freq_dicts[1]

    sum_raw = 0
    for w in freq_dict_raw:
        sum_raw += freq_dict_raw[w]

    print(sum_raw)
    print(len(freq_dict_raw))
    print(len(freq_dict_lemmas))

    freqs_raw = [freq_dict_raw[w] / sum_raw for w in freq_dict_raw]
    freqs_lemmas = [freq_dict_lemmas[w] / sum_raw for w in freq_dict_lemmas]

    # MLE entropy
    entropy_raw = -sum([pk * math.log(pk) / math.log(base) for pk in freqs_raw])
    entropy_lemmas = -sum([pk * math.log(pk) / math.log(base) for pk in freqs_lemmas])

    return sum_raw, len(freq_dict_raw), len(freq_dict_lemmas), round(entropy_raw, 3), round(entropy_lemmas, 3)

#read_text('UD/ud-treebanks-v2.3/UD_Afrikaans-AfriBooms/af_afribooms-ud-dev.conllu')
#print(unigram_entropy())
read_all(path)
