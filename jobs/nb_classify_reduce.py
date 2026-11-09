"""Job 4 · Clasificar — reduce (UNO solo, para ver todos los conteos juntos).

Cuenta cada combinación real->predicha (matriz de confusión) y al final calcula la exactitud.
"""
import sys
import texto  # noqa: F401

conteo = {}
for linea in sys.stdin:
    clave, n = linea.rstrip("\n").split("\t")
    conteo[clave] = conteo.get(clave, 0) + int(n)

for clave in sorted(conteo):
    print(f"{clave}\t{conteo[clave]}")
total = sum(conteo.values())
aciertos = conteo.get("pos->pos", 0) + conteo.get("neg->neg", 0)
if total:
    print(f"exactitud\t{aciertos}/{total}\t{aciertos / total:.1%}")
