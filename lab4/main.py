import os
import subprocess
import sys
from pathlib import Path
from typing import Literal
from my_search import SearchEngine, register_sample_documents
from pagerank import SEPageRank, SEPregelPageRank

filename = "file.db"

engine = SearchEngine(filename)
register_sample_documents(engine)

pagerank = SEPageRank(engine)
pregel_pagenrank = SEPregelPageRank(engine)


def view_documents(eng: SearchEngine):
    documents = eng.get_documents(True)
    size = len(documents)
    if size == 0:
        print("В поисковую систему не добавлено ни одного документа")
        return
    
    print(f"Добавленные документы ({size} шт.):")
    for doc in documents:
        print(f"| ID{doc[0]} - {doc[1]}")

Search_Types = Literal["daat", "taat"]
def run_search(eng: SearchEngine, search_type: Search_Types):
    term = str(input("Введите запрос: ")).strip()

    if search_type == "daat":
        print(f"Поиск Document-at-a-time по {term}:")
        engine.search_daat(term)
    elif search_type == "taat":
        print(f"Поиск Term-at-a-time по {term}:")
        engine.search_taat(term)

actions = [
    {"name": 'Просмотр документов', "exec": lambda: view_documents(engine)},
    {"name": "Парсинг", "exec": lambda: engine.parse()},
    {"name": "PageRank (MapReduce)", "exec": lambda: pagerank.run()},
    {"name": "PageRank (Pregel)", "exec": lambda: pregel_pagenrank.run()},
    {"name": "Поиск (Term-at-a-time)", "exec": lambda: run_search(engine, "taat")},
    {"name": "Поиск (Document-at-a-time)", "exec": lambda: run_search(engine, "daat")},
    {"name": "Выход", "exec": lambda: engine.exit(), "id":0}
]

def print_action_list():
    action_id = 1
    for action in actions:
        a_id = action_id
        if "id" in action:
            a_id = action["id"]
        else:
            action["id"] = a_id
            action_id = a_id + 1
        print(f"{a_id}. {action["name"]}")

if __name__ == "__main__":
    if not os.path.exists(filename):
        try:
            Path(filename).touch()
        except:
            ...
    
    print_action_list()
    actions_len = len(actions)
    while True:
        id = input("Введите номер действия: ")
        if id.strip() == "":
            print_action_list()
            continue
        id = int(id)
        if id < 0 or id >= actions_len:
            print("Действие не найдено")
        for i in range(len(actions)):
            action = actions[i]
            if "id" in action and action["id"] == id:
                actions[i]["exec"]()
        print()