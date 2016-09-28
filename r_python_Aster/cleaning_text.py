#!/usr/bin/env python3.4
import sys
import string


stopword_english = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours',
'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers',
'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves',
'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are',
'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does',
'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until',
'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into',
'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down',
'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here',
'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more',
'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so',
'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now',
'dont', 'doesnt', 'havent','hasnt', 'youre', 'its','hes','shes', 'im',
'wasnt', 'werent']

exclude = set(string.punctuation)

def stem(word):
    for suffix in ['ing', 'ly', 'ed', 'ious', 'ies', 'ive', 'es', 's', 'ment']:
        if word.endswith(suffix):
            return word[:-len(suffix)]
    return word

def preprocess(sentence):
    sentence = sentence.lower().replace('<br />','')
    sentence = ''.join(ch for ch in sentence if ch not in exclude)
    tokens = sentence.split(" ")
    filtered_words = list(filter(lambda token: token not in stopword_english, tokens))
    stem_words = map(lambda token: stem(token),filtered_words)
    return " ".join(stem_words)

_lines = []
_new_text = []
while True:
    line = sys.stdin.readline()
    # Break on EOF.
    if  line == "":
        break
    print('%s\t%s' % (line.rstrip('\n').rstrip('\t'),preprocess(line).rstrip('\n').rstrip('\t')))     
        
        
        
sys.stdout.flush()
              