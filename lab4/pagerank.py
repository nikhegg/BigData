from collections import defaultdict
from my_search import SearchEngine
from libs import pregel

def extract_data(search_engine: SearchEngine):
    id_to_url = {}
    url_to_id = {}
    docs = search_engine.get_documents(True)
    for doc_id, url in docs:
        id_to_url[doc_id] = url
        url_to_id[url] = doc_id
    links_dict = {doc_id: [] for doc_id in id_to_url.keys()}

    links = search_engine.get_links()
    for _, from_id, to_url in links:
        to_id = url_to_id.get(to_url)
        if to_id is not None and from_id in links_dict:
            links_dict[from_id].append(to_id)
    
    return links_dict, id_to_url

def sepr_map(links_dict, ranks):
    contributions = defaultdict(float)
    dangling_mass = 0.0

    for doc_from, neighbours in links_dict.items():
        rank_from = ranks[doc_from]
        if neighbours:
            share = rank_from / len(neighbours)
            for doc_to in neighbours:
                contributions[doc_to] += share
        else:
            dangling_mass += rank_from

    return contributions, dangling_mass

def sepr_reduce(links_dict, contributions, dangling_mass, pages, alpha=0.85):
    ranks = {}

    base = (1.0 - alpha) / pages
    dangling_share = (alpha * dangling_mass) / pages

    all_pages = links_dict.keys()
    
    for doc_id in all_pages:
        contribution = contributions.get(doc_id, 0.0)
        ranks[doc_id] = base + dangling_share + alpha * contribution

    return ranks

class SEPageRank():
    def __init__(self, engine: SearchEngine):
        self.engine = engine

    def run(self, iterations=20, alpha=0.85):
        links_dict, id_to_url = extract_data(self.engine)
        if not links_dict:
            print("Связей не найдено")
            return
        
        # Pagerank
        pages = len(links_dict)
        ranks = {doc_id: 1.0 / pages for doc_id in links_dict.keys()}
        for _ in range(iterations):
            contributions, dangling_mass = sepr_map(links_dict, ranks)
            ranks = sepr_reduce(links_dict, contributions, dangling_mass, pages, alpha)

        ranked = sorted(ranks.items(), key=lambda x: x[1], reverse=True)

        print("PageRank:")
        for doc_id, rank in ranked:
            url = id_to_url.get(doc_id, "???")
            print(f"| ID{doc_id} - PageRank={rank:.5f}, URL={url}")

        

class PageRankVertex(pregel.Vertex):
    def __init__(self, doc_id, out_vertices, num_vertices, alpha=0.85, all_vertices=None):
        initial_value = 1.0 / num_vertices if num_vertices > 0 else 0.0
        super(PageRankVertex, self).__init__(doc_id, initial_value, out_vertices)
        self.num_vertices = num_vertices
        self.damping = alpha
        self.all_vertices = all_vertices

    def update(self):
        if self.superstep >= 20: # Максимум superstep
            self.active = False
            self.outgoing_messages = []
            return

        self.outgoing_messages = []

        if self.superstep == 0:
            self._send_rank()
            return

        incoming_sum = 0.0
        for _, value in self.incoming_messages:
            incoming_sum += value

        base = (1.0 - self.damping) / self.num_vertices if self.num_vertices > 0 else 0.0
        new_rank = base + self.damping * incoming_sum

        self.value = new_rank

        self._send_rank()

    def _send_rank(self):
        if self.num_vertices == 0:
            return

        if self.out_vertices:
            share = self.value / float(len(self.out_vertices))
            for neighbour in self.out_vertices:
                self.outgoing_messages.append((neighbour, share))
        else:
            if self.all_vertices:
                share = self.value / float(self.num_vertices)
                for page in self.all_vertices:
                    self.outgoing_messages.append((page, share))

class SEPregelPageRank():
    def __init__(self, engine: SearchEngine):
        self.engine = engine

    def run(self):
        links_dict, id_to_url = extract_data(self.engine)
        if not links_dict:
            print("Связей не найдено")
            return
        
        num_vertices = len(links_dict)
        vertices_by_id = {}
        vertices = []

        for doc_id in links_dict.keys():
            vertex = PageRankVertex(doc_id, [], num_vertices)
            vertices_by_id[doc_id] = vertex
            vertices.append(vertex)

        for vertex in vertices:
            vertex.all_vertices = vertices

        for doc_id, neighbours_ids in links_dict.items():
            vertex = vertices_by_id[doc_id]
            vertex.out_vertices = [vertices_by_id[neighbour_id] for neighbour_id in neighbours_ids]

        # use vertices array
        pregel.Pregel(vertices, 4).run()

        ranks = {}
        for vertex in vertices:
            ranks[vertex.id] = vertex.value
        
        ranked = sorted(ranks.items(), key=lambda x: x[1], reverse=True)

        print("Pregel PageRank:")
        for doc_id, rank in ranked:
            url = id_to_url.get(doc_id, "???")
            print(f"| ID{doc_id} - PageRank={rank:.5f}, URL={url}")