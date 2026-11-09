"""Job 1 · WordCount — reduce: suma los 1 de cada palabra.

Hadoop entrega las líneas ORDENADAS por clave, así que todas las de una palabra llegan
seguidas: basta con acumular hasta que la clave cambie. También sirve como combiner,
porque sumar sumas parciales da el mismo resultado.
"""
import sys
import texto  # noqa: F401  (configura UTF-8)

actual, total = None, 0
for linea in sys.stdin:
    palabra, n = linea.rstrip("\n").split("\t")
    if palabra != actual:
        if actual is not None:
            print(f"{actual}\t{total}")
        actual, total = palabra, 0
    total += int(n)
if actual is not None:
    print(f"{actual}\t{total}")
