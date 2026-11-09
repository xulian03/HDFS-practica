"""Job 3 · Entrenar clasificador — map.

Solo usa reseñas de ENTRENAMIENTO (no de test) con nota clara (pos: 7-10, neg: 1-4).
Por cada palabra emite `palabra\tpos_count\tneg_count`, y por cada reseña una línea
especial `__DOCS__` para contar cuántas reseñas hay de cada clase (la "probabilidad a priori").
"""
import sys
import texto

for linea in sys.stdin:
    fila = texto.parsear(linea)
    if not fila or texto.es_test(fila):
        continue
    clase = texto.etiqueta(fila["review_rate"])
    if clase is None:
        continue
    uno = "1\t0" if clase == "pos" else "0\t1"
    print(f"__DOCS__\t{uno}")
    for palabra in texto.tokenizar(fila["review_title"] + " " + fila["review_text"]):
        print(f"{palabra}\t{uno}")
