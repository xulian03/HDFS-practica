"""Job 3 · Entrenar clasificador — reduce. Salida = el "modelo":

    palabra \t veces_en_reseñas_positivas \t veces_en_reseñas_negativas

Descarta palabras que aparecen menos de 3 veces (ruido y nombres raros).
"""
import sys
import texto  # noqa: F401

MIN_APARICIONES = 3


def emitir(clave, pos, neg):
    if clave == "__DOCS__" or pos + neg >= MIN_APARICIONES:
        print(f"{clave}\t{pos}\t{neg}")


actual, pos, neg = None, 0, 0
for linea in sys.stdin:
    palabra, p, n = linea.rstrip("\n").split("\t")
    if palabra != actual:
        if actual is not None:
            emitir(actual, pos, neg)
        actual, pos, neg = palabra, 0, 0
    pos += int(p)
    neg += int(n)
if actual is not None:
    emitir(actual, pos, neg)
