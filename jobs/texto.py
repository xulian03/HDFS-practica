"""Utilidades compartidas por todos los mappers y reducers.

Hadoop Streaming copia este archivo (con -files) al directorio de trabajo de cada tarea,
así que los scripts pueden hacer `import texto` aunque corran en cualquier NodeManager.
"""
import re
import sys
import zlib

# Hadoop Streaming entrega bytes; forzamos UTF-8 sin importar el locale del contenedor.
# newline="\n" evita que en Windows (pruebas locales) se escriba \r\n.
sys.stdin.reconfigure(encoding="utf-8", errors="replace")
sys.stdout.reconfigure(encoding="utf-8", newline="\n")

SEP = "||"
COLUMNAS = ["film_name", "gender", "film_avg_rate", "review_rate", "review_title", "review_text"]

STOPWORDS = set("""
a al algo algunas algunos ante antes aquel aquella aquellas aquello aquellos aqui así asi aun aunque
bajo bien cada casi como cómo con contra cual cuales cuando cuándo cuanto de del desde donde dónde
dos durante el él ella ellas ello ellos en entre era eran eres es esa esas ese eso esos esta está
están estaba estado estar estas este esto estos fue fueron fui ha había habia han has hasta hay he
hemos hace hacen hacer hacia la las le les lo los más mas me mi mí mis mucho muchos muy nada ni no
nos nosotros nuestra nuestro o os otra otras otro otros para pero poco por porque que qué quien
quién se sea ser si sí sido sin sobre solo sólo son su sus tal también tambien tan tanto te tener
tiene tienen todo todos tras tu tú tus un una uno unos unas usted vez y ya yo
película pelicula películas peliculas film cine
""".split())

_PALABRA = re.compile(r"[a-záéíóúüñ]+")


def parsear(linea):
    """Devuelve un dict con las 6 columnas, o None si es la cabecera o una línea rota."""
    partes = linea.rstrip("\n").split(SEP)
    if len(partes) != 6 or partes[0] == "film_name":
        return None
    return dict(zip(COLUMNAS, partes))


def tokenizar(texto):
    """Minúsculas, solo letras, sin stopwords ni palabras de menos de 3 letras."""
    return [p for p in _PALABRA.findall(texto.lower()) if len(p) > 2 and p not in STOPWORDS]


def etiqueta(nota):
    """Nota 7-10 → pos, 1-4 → neg, 5-6 (o vacía) → None (neutra, se descarta)."""
    if not nota.isdigit():
        return None
    n = int(nota)
    if n >= 7:
        return "pos"
    if n <= 4:
        return "neg"
    return None


def es_test(fila):
    """Separa ~20% de las reseñas para evaluar el clasificador.

    Usamos crc32 y NO hash() de Python: hash() cambia en cada proceso, y cada mapper es un
    proceso distinto en un nodo distinto. Con crc32 todas las tareas coinciden en qué es test.
    """
    clave = (fila["film_name"] + fila["review_title"] + fila["review_text"]).encode("utf-8")
    return zlib.crc32(clave) % 10 < 2
