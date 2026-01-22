from __future__ import annotations
from collections import deque
from typing import List, Optional, Dict, Tuple
from tools.schema_graph import SchemaGraph, FKEdge


def shortest_path_fk(graph: SchemaGraph, src: str, dst: str) -> Optional[List[FKEdge]]:
    """
    BFS sur les arêtes FK (sens src->dst).
    On retourne une liste d'edges: src -> ... -> dst
    """
    if src == dst:
        return []

    q = deque([src])
    prev: Dict[str, Tuple[str, FKEdge]] = {}  

    while q:
        cur = q.popleft()
        for e in graph.out_edges.get(cur, []):
            nxt = e.dst_table
            if nxt in prev or nxt == src:
                continue
            prev[nxt] = (cur, e)
            if nxt == dst:
                path: List[FKEdge] = []
                node = dst
                while node != src:
                    pnode, pedge = prev[node]
                    path.append(pedge)
                    node = pnode
                path.reverse()
                return path
            q.append(nxt)

    return None
