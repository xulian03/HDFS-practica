"""Job 3 · Top N — map: emite `película\t1` por cada reseña (igual que WordCount, pero la clave es la película)."""
import sys
import texto

for linea in sys.stdin:
    fila = texto.parsear(linea)
    if fila:
        print(f"{fila['film_name']}\t1")
