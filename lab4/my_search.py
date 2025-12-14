from bs4 import BeautifulSoup
from collections import Counter, defaultdict
import math
import os
import re
import requests
import sqlite3
import sys
from pathlib import Path
from urllib.parse import urljoin

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

def prepare_terms(text):
    text = text.lower()
    return re.findall(r"[а-яА-Яa-zA-Z0-9]+", text)

class SearchEngine():
    def __init__(self, dbpath):
        self.documents = []
        if not os.path.exists(dbpath):
            Path(dbpath).touch()
        self.dbpath = dbpath
        self.connection = sqlite3.connect(dbpath)
        self.cursor = self.connection.cursor()

        self.cursor.execute("""CREATE TABLE IF NOT EXISTS documents (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            url  TEXT UNIQUE
        )""")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS words (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT UNIQUE
        )""")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS doc_words (
            doc_id INTEGER,
            word_id INTEGER,
            count  INTEGER,
            PRIMARY KEY (doc_id, word_id),
            FOREIGN KEY (doc_id) REFERENCES documents(id),
            FOREIGN KEY (word_id) REFERENCES words(id)
        )""")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_doc_id INTEGER,
            to_url TEXT,
            FOREIGN KEY (from_doc_id) REFERENCES documents(id)
        )""")
        self.connection.commit()

        self.cursor.execute("SELECT url FROM documents")
        urls = self.cursor.fetchall()
        for url in urls:
            self.documents.append(url[0])

    # ==================== Documents ====================
    def get_documents(self, include_id: bool=False):
        if include_id:
            self.cursor.execute("SELECT * FROM documents")
            return self.cursor.fetchall()
        else:
            return self.documents
        
    def has_document(self, document_url: str):
        for i in range(len(self.documents)):
            if self.documents[i] == document_url:
                return True
        return False

    def register_document(self, document_url: str):
        if self.has_document(document_url):
            self.cursor.execute("SELECT id FROM documents WHERE url=?", (document_url,))
            return self.cursor.fetchone()[0]
        
        self.documents.append(document_url)
        self.cursor.execute("INSERT INTO documents (url) VALUES (?)", (document_url,))
        return self.cursor.lastrowid

    def unregister_document(self, document_url: str):
        try:
            self.documents.remove(document_url)
            self.cursor.execute("REMOVE FROM documents WHERE url=?", (document_url,))
        except:
            ...
    # ===================================================


    # ====================== Words ======================
    def get_words(self):
        self.cursor.execute("SELECT * FROM words")
        return self.cursor.fetchall()

    def register_word(self, word):
        self.cursor.execute("SELECT id FROM words WHERE word=?", (word,))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        self.cursor.execute("INSERT INTO words (word) VALUES (?)", (word,))
        return self.cursor.lastrowid
    
    def unregister_word(self, word):
        self.cursor.execute("REMOVE FROM words WHERE word=?", (word,))
    # ===================================================

    def get_links(self):
        self.cursor.execute("SELECT * FROM links")
        return self.cursor.fetchall()

    def parse(self):
        docs = {}
        for url in self.documents:
            print(f"- Парсим документ {url}")
            try:
                res = requests.get(url, headers=HEADERS, timeout=10)
                res.raise_for_status()
                html = res.text
            except:
                continue

            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()

            text = soup.get_text()
            text = re.sub(r"\s+", " ", text).strip()

            tokens = re.findall(r"[а-яА-Яa-zA-Z0-9]+", text.lower())
            word_counts = Counter(tokens)

            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                full_url = urljoin(url, href)
                links.append(full_url)

            docs[url] = {
                "words": word_counts,
                "links": links  
            }
        print("Парсинг документов окончен")
        
        # Save to DB
        for url, data in docs.items():
            doc_id = self.register_document(url)
            print(f"- Сохранение документа {url} (ID{doc_id})")

            for word, count in data["words"].items():
                word_id = self.register_word(word)
                self.cursor.execute("INSERT OR REPLACE INTO doc_words (doc_id, word_id, count) VALUES (?,?,?)", (doc_id, word_id, count))

            for link in data["links"]:
                self.cursor.execute("INSERT INTO links (from_doc_id, to_url) VALUES (?, ?)", (doc_id, link))

        self.connection.commit()
        print(f"Данные парсинга сохранены в {self.dbpath}")

    def search_taat(self, terms: str="", size:int=10):
        terms = prepare_terms(terms)
        docs = self.get_documents(True)
        docs_size = len(docs)
        id_to_url = {}
        for doc_id, url in docs:
            id_to_url[doc_id] = url

        index = defaultdict(list)        
        self.cursor.execute("""
            SELECT words.word, doc_words.doc_id, doc_words.count FROM doc_words
            JOIN words ON words.id = doc_words.word_id
        """)
        for word, doc_id, count in self.cursor.fetchall():
            index[word].append((doc_id, count))
        for word in index:
            index[word].sort(key=lambda x: x[0])

        if terms == "":
            print("Ничего не найдено")
            return
        scores = defaultdict(float)

        for term in terms:
            term_docs = index.get(term)
            if not term_docs:
                continue

            term_docs_size = len(term_docs) 
            if term_docs_size == 0:
                continue

            idf = math.log(float(docs_size) / term_docs_size)

            for doc_id, tf in term_docs:
                scores[doc_id] += tf * idf

        if not scores:
            print("Документов по запросу не найдено")
            return
        
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:size]

        for doc_id, score in ranked:
            url = id_to_url.get(doc_id, "???")
            print(f"| ID{doc_id} - Score={score:.3f}, URL={url}")

    def search_daat(self, terms: str="", size:int=10):
        terms = prepare_terms(terms)
        docs = self.get_documents(True)
        docs_size = len(docs)
        id_to_url = {}
        for doc_id, url in docs:
            id_to_url[doc_id] = url

        index = defaultdict(list)        
        self.cursor.execute("""
            SELECT words.word, doc_words.doc_id, doc_words.count FROM doc_words
            JOIN words ON words.id = doc_words.word_id
        """)
        for word, doc_id, count in self.cursor.fetchall():
            index[word].append((doc_id, count))
        for word in index:
            index[word].sort(key=lambda x: x[0])

        if terms == "":
            print("Ничего не найдено")
            return
        
        term_docs = {}
        for term in terms:
            docs_for_term = index.get(term)
            if docs_for_term:
                term_docs[term] = docs_for_term

        if not term_docs:
            print("Документов по запросу не найдено")        
            return
        
        scores = defaultdict(float)
        pointers = {term: 0 for term in term_docs.keys()}

        while True:
            cur_doc_ids = []
            for term, docs_for_term in term_docs.items():
                pos = pointers[term]
                if pos < len(docs_for_term):
                    doc_id, _ = docs_for_term[pos]
                    cur_doc_ids.append(doc_id)

            if not cur_doc_ids:
                break
            
            cur_doc = min (cur_doc_ids)
            for term, docs_for_term in term_docs.items():
                pos = pointers[term]
                if pos >= len(docs_for_term):
                    continue
                
                doc_id, tf = docs_for_term[pos]
                if doc_id == cur_doc:
                    df = len(docs_for_term)
                    if df > 0:
                        idf = math.log(float(docs_size) / df)
                        scores[doc_id] += tf * idf
                    pointers[term] += 1

        if not scores:
            print("Документов по запросу не найдено")
            return

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:size]
        for doc_id, score in ranked:
            url = id_to_url.get(doc_id, "???")
            print(f"| ID{doc_id} - Score={score:.3f}, URL={url}")

    def exit(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()
        print(f"База данных сохранена в {self.dbpath}")
        sys.exit(0)



# Samples
sample_documents = [
    "https://ru.wikipedia.org/wiki/Фреймворк",
    "https://ru.wikipedia.org/wiki/MapReduce",
    "https://ru.wikipedia.org/wiki/Python",
    "https://ru.wikipedia.org/wiki/Java",
    "https://ru.wikipedia.org/wiki/Ubuntu",
    "https://ru.wikipedia.org/wiki/Nvidia",
    "https://ru.wikipedia.org/wiki/YouTube",
    "https://ru.wikipedia.org/wiki/Google"
]
def register_sample_documents(engine: SearchEngine):
    for doc in sample_documents:
        engine.register_document(doc)