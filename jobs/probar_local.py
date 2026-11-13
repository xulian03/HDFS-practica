"""Simula en tu PC lo que hace Hadoop Streaming, sin clúster:

    cat input | mapper | sort | reducer

Sirve para depurar los scripts antes de mandarlos a YARN. Uso:

    python jobs/probar_local.py            # corre los 3 jobs
    python jobs/probar_local.py wordcount  # solo uno (wordcount, promedio o top)
"""
import pathlib
import shutil
import subprocess
import sys
import tempfile

JOBS = pathlib.Path(__file__).resolve().parent
DATASET = JOBS.parent / "datasets" / "reviews_filmaffinity.csv"


def etapa(script, entrada, cwd):
    r = subprocess.run([sys.executable, script], input=entrada, capture_output=True, cwd=cwd)
    if r.returncode:
        sys.exit(f"{script} falló:\n{r.stderr.decode('utf-8', 'replace')}")
    return r.stdout


def correr(mapper, reducer, cwd, entrada):
    salida_map = etapa(mapper, entrada, cwd)
    # El "shuffle & sort": Hadoop ordena por clave antes de llamar al reducer.
    ordenado = b"".join(sorted(salida_map.splitlines(keepends=True)))
    return etapa(reducer, ordenado, cwd).decode("utf-8")


def main():
    pedido = sys.argv[1] if len(sys.argv) > 1 else "todos"
    entrada = DATASET.read_bytes()
    with tempfile.TemporaryDirectory() as tmp:
        for f in JOBS.glob("*.py"):
            shutil.copy(f, tmp)  # igual que -files: todo en el mismo directorio de trabajo

        if pedido in ("todos", "wordcount"):
            out = correr("wc_map.py", "wc_reduce.py", tmp, entrada)
            filas = sorted((l.split("\t") for l in out.splitlines()), key=lambda x: -int(x[1]))
            print("== Job 1 · WordCount (top 15) ==")
            for palabra, n in filas[:15]:
                print(f"{palabra}\t{n}")

        if pedido in ("todos", "promedio"):
            print("\n== Job 2 · Promedio de nota por género ==")
            print(correr("prom_map.py", "prom_reduce.py", tmp, entrada))

        if pedido in ("todos", "top"):
            # En el clúster el combiner es wc_reduce.py; aquí se omite: el resultado es el mismo.
            print("\n== Job 3 · Top 10 películas con más reseñas ==")
            print(correr("top_map.py", "top_reduce.py", tmp, entrada))


if __name__ == "__main__":
    main()
