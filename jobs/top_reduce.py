"""Job 3 · Top N — reduce: con UN solo reducer, cuenta las reseñas de cada película y emite las 10 mayores.

Las líneas llegan ordenadas por clave: se acumula hasta que la película cambia. Para quedarse con
el Top 10 no hace falta guardar todas las películas: un montículo (heap) de tamaño 10 descarta la menor
cada vez que entra una mayor. Necesita 1 reducer: con varios, cada uno vería solo parte de las películas
y habría que combinar los Top 10 parciales.

Como combiner se usa wc_reduce.py (sumar sumas parciales es correcto); este reducer NO sirve como
combiner, porque descartaría películas que en otro mapper sí sumarían.
"""
import heapq
import sys
import texto  # noqa: F401  (configura UTF-8)

N = 10
top = []  # montículo mínimo de (reseñas, película)


def guardar(pelicula, total):
    item = (total, pelicula)
    if len(top) < N:
        heapq.heappush(top, item)
    else:
        heapq.heappushpop(top, item)


actual, total = None, 0
for linea in sys.stdin:
    pelicula, n = linea.rstrip("\n").split("\t")
    if pelicula != actual:
        if actual is not None:
            guardar(actual, total)
        actual, total = pelicula, 0
    total += int(n)
if actual is not None:
    guardar(actual, total)

for total, pelicula in sorted(top, reverse=True):
    print(f"{pelicula}\t{total}")
