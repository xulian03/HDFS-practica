#!/usr/bin/env bash
# Lanza los jobs MapReduce (Hadoop Streaming) desde el contenedor client:
#
#   docker compose exec client bash /workspace/jobs/run.sh setup        # sube el dataset a HDFS
#   docker compose exec client bash /workspace/jobs/run.sh wordcount
#   docker compose exec client bash /workspace/jobs/run.sh promedio
#   docker compose exec client bash /workspace/jobs/run.sh entrenar
#   docker compose exec client bash /workspace/jobs/run.sh clasificar   # requiere "entrenar"
#   docker compose exec client bash /workspace/jobs/run.sh todos
#
# Estructura en HDFS:
#   /reviews/input/reviews_filmaffinity.csv
#   /reviews/output/01-wordcount  02-promedio-genero  03-modelo  04-clasificacion
set -euo pipefail

STREAMING=$(ls /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar)
J=/workspace/jobs
IN=/reviews/input
OUT=/reviews/output

# run <nombre> <opciones...>: borra la salida anterior (Hadoop se niega a sobrescribir) y lanza el job.
run() {
  local nombre=$1; shift
  hdfs dfs -rm -r -f -skipTrash "$OUT/$nombre" >/dev/null
  echo ">>> Job $nombre"
  # -cmdenv PYTHONPATH=. : -files deja cada archivo en su propio directorio de caché y solo pone
  # enlaces simbólicos en el directorio de la tarea. Python resuelve el enlace del script y busca
  # módulos en la carpeta real, donde texto.py NO está → ModuleNotFoundError. Con PYTHONPATH=.
  # también busca en el directorio de la tarea, donde sí está el enlace a texto.py.
  hadoop jar "$STREAMING" -D mapreduce.job.name="$nombre" "$@" -cmdenv PYTHONPATH=. \
    -input "$IN" -output "$OUT/$nombre"
  echo ">>> Salida en HDFS:"
  hdfs dfs -ls "$OUT/$nombre"
}

case "${1:-}" in
  setup)
    hdfs dfs -mkdir -p "$IN" "$OUT"
    hdfs dfs -put -f /workspace/datasets/reviews_filmaffinity.csv "$IN/"
    hdfs dfs -ls "$IN"
    hdfs fsck "$IN/reviews_filmaffinity.csv" -files -blocks -locations | tail -n 20
    ;;
  wordcount)
    # 3 reducers → 3 archivos part-0000N; cada palabra cae en uno solo (particionado por hash).
    # El reducer también hace de combiner: suma parcial en cada mapper antes del shuffle.
    run 01-wordcount \
      -files "$J/texto.py,$J/wc_map.py,$J/wc_reduce.py" \
      -numReduceTasks 3 \
      -mapper "python3 wc_map.py" -combiner "python3 wc_reduce.py" -reducer "python3 wc_reduce.py"
    echo ">>> Top 15:"
    hdfs dfs -cat "$OUT/01-wordcount/part-*" | sort -t$'\t' -k2,2nr | head -15
    ;;
  promedio)
    run 02-promedio-genero \
      -files "$J/texto.py,$J/prom_map.py,$J/prom_reduce.py" \
      -mapper "python3 prom_map.py" -reducer "python3 prom_reduce.py"
    hdfs dfs -cat "$OUT/02-promedio-genero/part-*"
    ;;
  entrenar)
    # 1 reducer → un solo archivo de modelo que luego se reparte a todos los mappers.
    run 03-modelo \
      -files "$J/texto.py,$J/nb_train_map.py,$J/nb_train_reduce.py" \
      -numReduceTasks 1 \
      -mapper "python3 nb_train_map.py" -reducer "python3 nb_train_reduce.py"
    # 2>/dev/null: head cierra la tubería tras 5 líneas y cat se queja; es inofensivo.
    hdfs dfs -cat "$OUT/03-modelo/part-00000" 2>/dev/null | head -5
    ;;
  clasificar)
    # "hdfs://...#modelo.tsv": Hadoop descarga el modelo desde HDFS a cada nodo (distributed cache)
    # y lo deja en el directorio de la tarea con el nombre modelo.tsv.
    run 04-clasificacion \
      -files "$J/texto.py,$J/nb_classify_map.py,$J/nb_classify_reduce.py,hdfs://namenode:9000$OUT/03-modelo/part-00000#modelo.tsv" \
      -numReduceTasks 1 \
      -mapper "python3 nb_classify_map.py" -reducer "python3 nb_classify_reduce.py"
    hdfs dfs -cat "$OUT/04-clasificacion/part-00000"
    ;;
  fallar)
    # Job roto a propósito: el mapper no existe. YARN reintenta cada map task (4 intentos)
    # y al final marca el job como FAILED. Útil para el punto 3 (ver estados y leer logs).
    run 99-fallido \
      -files "$J/texto.py" \
      -mapper "python3 no_existe.py" -reducer "cat" || echo ">>> El job falló, como se esperaba"
    ;;
  todos)
    for j in setup wordcount promedio entrenar clasificar; do "$0" "$j"; done
    ;;
  *)
    sed -n '2,13p' "$0"; exit 1
    ;;
esac
