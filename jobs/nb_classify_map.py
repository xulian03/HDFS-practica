"""Job 4 · Clasificar — map.

Carga el modelo (`modelo.tsv`, que Hadoop copia a cada nodo con -files desde HDFS) y
clasifica las reseñas de TEST con Naive Bayes:

    puntaje(clase) = log P(clase) + Σ log P(palabra | clase)

Emite `real->predicha\t1` para que el reducer arme la matriz de confusión.
"""
import math
import sys
import texto

pos_w, neg_w = {}, {}
docs = {"pos": 0, "neg": 0}
with open("modelo.tsv", encoding="utf-8") as f:
    for linea in f:
        palabra, p, n = linea.rstrip("\n").split("\t")
        if palabra == "__DOCS__":
            docs = {"pos": int(p), "neg": int(n)}
        else:
            pos_w[palabra], neg_w[palabra] = int(p), int(n)

vocab = len(pos_w)
total = {"pos": sum(pos_w.values()), "neg": sum(neg_w.values())}
conteos = {"pos": pos_w, "neg": neg_w}
prior = {c: math.log(docs[c] / (docs["pos"] + docs["neg"])) for c in docs}


def puntaje(palabras, clase):
    # Suavizado de Laplace (+1): una palabra nunca vista en una clase no anula la probabilidad.
    denom = total[clase] + vocab
    return prior[clase] + sum(math.log((conteos[clase].get(w, 0) + 1) / denom)
                              for w in palabras if w in pos_w)


for linea in sys.stdin:
    fila = texto.parsear(linea)
    if not fila or not texto.es_test(fila):
        continue
    real = texto.etiqueta(fila["review_rate"])
    if real is None:
        continue
    palabras = texto.tokenizar(fila["review_title"] + " " + fila["review_text"])
    predicha = "pos" if puntaje(palabras, "pos") >= puntaje(palabras, "neg") else "neg"
    print(f"{real}->{predicha}\t1")
