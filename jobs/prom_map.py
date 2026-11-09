"""Job 2 · Promedio por clave — map: emite `género\tnota` por cada reseña con nota."""
import sys
import texto

for linea in sys.stdin:
    fila = texto.parsear(linea)
    if fila and fila["review_rate"].isdigit():
        print(f"{fila['gender']}\t{fila['review_rate']}")
