from map_reduce.client.server_interface import ServerInterface as server

'''
MapReduce function schema.
def map(in_key: KEY, in_value: VALUE) -> [(KEY, VALUE)]:
    pass

def reduce(out_key: KEY, inter_value: VALUE) -> [VALUE]:
    pass
'''
data = []
with open('map_reduce/client/data.txt') as file:
    data = [ line[:-1] for line in file.readlines() ]

def map(doc_line: int, doc_line_text: str) -> list[tuple[str, int]]:
    res = []
    for word in doc_line_text.split():
        res.append((word, 1))
    return res

def reduce(word: str, vals: list[int]) -> int:
    count = 0
    for v in vals:
        count += v
    return count

def run_client():
    if daemon := server.startup(data, map, reduce):
        print( 'MapReduce tasks started, awaiting results...' )
        server.await_results()
        print( server.results )