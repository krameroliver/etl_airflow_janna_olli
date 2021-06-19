from itertools import islice

def chunk(it, size):
    it = iter(it)
    return iter(lambda: islice(it, size), ())


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]