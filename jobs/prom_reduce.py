"""Job 2 · Promedio por clave — reduce: `género\tnúmero_de_reseñas\tpromedio`.

OJO: este reducer NO sirve como combiner. El promedio de promedios parciales no es el
promedio total (un mapper con 1 reseña pesaría igual que otro con 1.000).
"""
import sys
import texto  # noqa: F401


def emitir(clave, suma, n):
    print(f"{clave}\t{n}\t{suma / n:.2f}")


actual, suma, n = None, 0, 0
for linea in sys.stdin:
    genero, nota = linea.rstrip("\n").split("\t")
    if genero != actual:
        if actual is not None:
            emitir(actual, suma, n)
        actual, suma, n = genero, 0, 0
    suma += int(nota)
    n += 1
if actual is not None:
    emitir(actual, suma, n)
