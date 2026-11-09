"""Job 1 · WordCount — map: por cada palabra del texto de la reseña emite `palabra\t1`."""
import sys
import texto

for linea in sys.stdin:
    fila = texto.parsear(linea)
    if fila:
        for palabra in texto.tokenizar(fila["review_title"] + " " + fila["review_text"]):
            print(f"{palabra}\t1")
