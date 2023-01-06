import requests
from tqdm import tqdm
import os
from lm_dataformat import Archive
import shutil
import spacy
import json
import glob
import sys
import html2text

def get_word_stats(txt):
    if not txt:
        return 0, 0, 0, 0, 0, 0

    sentences = 0
    words = 0
    verbs = 0
    nouns = 0
    punctuations = 0
    symbols = 0

    doc = nlp(txt)

    sentences = len(list(doc.sents))
    words = len([token.text for token in doc if not token.is_punct])
    nouns = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "NOUN")])
    verbs = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "VERB")])
    punctuations = len([token.text for token in doc if (token.is_punct or token.pos_ == "PUNCT")])
    symbols = len([token.text for token in doc if (token.pos_ == "SYM")])

    return sentences, words, verbs, nouns, punctuations, symbols


ar = Archive('./data')

file_name_zst = './saos.zst'
file_name_manifest = './saos.manifest'
nlp = spacy.load("pl_core_news_md")

total_len = 0
total_docs = 0
total_sentences = 0
total_words = 0
total_verbs = 0
total_nouns = 0
total_punctuations = 0
total_symbols = 0

next_link = "https://www.saos.org.pl/api/dump/judgments"
counter = 0

h = html2text.HTML2Text()
h.ignore_links = True
error = 0
http_code = 0
http_text = ""

while next_link:
    
    print("Processing: ", next_link)

    try:

        r = requests.get(next_link)
        http_code = r.status_code
        http_text = r.text

        data = r.json()
        links = data.get("links")
        next_link = ""

        for link in links:
            if link.get("rel") == "next":
                next_link = link.get("href")


    except Exception as e:
        print("Error: ", e)
        print("HTTP code: ", http_code)
        print("HTTP text: ", http_text)
        error += 1
        if error > 10:
            print("Too many errors. Exiting.")
            break
        continue

    items = data.get("items")
    for item in items:
        counter += 1
        print("Items: ", item.get("id"))
        txt = h.handle(item.get("textContent"))
        txt = txt.replace("\n", " ")

        l = len(txt)
        if l > 100000:
            nlp.max_length = len(txt) + 100
        sentences, words, verbs, nouns, punctuations, symbols = get_word_stats(txt.strip())
        total_words += words
        total_verbs += verbs
        total_nouns += nouns
        total_len += l
        total_docs += 1
        total_sentences += sentences
        total_punctuations += punctuations
        total_symbols += symbols
        meta = {'id' : item.get("id"), 'length': l, 'sentences': sentences, 'words': words, 'verbs': verbs, 'nouns': nouns, 'punctuations': punctuations, 'symbols': symbols}
        ar.add_data(txt.strip(), meta = meta)
    
    error = 0
    print("Avg len: ", total_len / total_docs)
    print("Total len in MB: ", total_len / 1024 / 1024)

ar.commit()
data_files= glob.glob('./data/*')
file_size = 0

for f in data_files:
    if f.endswith('.zst'):
        shutil.copy(f, os.path.join(file_name_zst))
        file_size = os.path.getsize(file_name_zst)

    os.remove(f)

manifest = {"project" : "SpeakLeash", "name": "SAOS", "description": "The SAOS service collects and provides data on the rulings of Polish courts. ", "license": "", "language": "pl", "file_size" : file_size, "sources": [{"name": "SAOS", "url": "https://www.saos.org.pl/", "license": ""}], "stats": {"documents": total_docs, "sentences": total_sentences, "words" : total_words, "nouns" : total_nouns, "verbs" : total_verbs, "characters": total_len, "punctuations" : total_punctuations, "symbols" : total_symbols}}
json_manifest = json.dumps(manifest, indent = 4) 

with open(file_name_manifest, 'w') as mf:
    mf.write(json_manifest)

sys.exit()




