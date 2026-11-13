# Paso a paso: probar el proyecto y tomar las capturas

Todo se hace desde la carpeta del proyecto. Para las capturas usa `Win + Shift + S` y guárdalas con el nombre que dice la guía (ej. `A1.png`).

> Necesitas Docker Desktop abierto. La primera vez, `docker compose build` descarga Hadoop y puede tardar bastante (~900 MB). Es normal: déjalo terminar.

## 1. Encender todo

```bash
docker compose build
docker compose up -d
docker compose ps
```

Espera ~1 minuto. Debe verse todo arriba (`namenode` y `resourcemanager` en *healthy*).

Abre estas páginas en el navegador:

- http://localhost:9870 (HDFS)
- http://localhost:8088 (YARN)

📸 **Captura 1:** el resultado de `docker compose ps`.
📸 **Captura 2:** http://localhost:9870 → menú *Datanodes*: deben verse 3 nodos vivos.
📸 **Captura 3:** http://localhost:8088 → *Nodes*: 3 nodos RUNNING.

## 2. Subir el archivo y correr los 3 jobs

```bash
docker compose exec client bash /workspace/jobs/run.sh setup
docker compose exec client bash /workspace/jobs/run.sh wordcount
docker compose exec client bash /workspace/jobs/run.sh promedio
docker compose exec client bash /workspace/jobs/run.sh top
```

📸 **Captura 4:** la terminal al terminar cada job (se ve el resultado).
📸 **Captura 5:** http://localhost:8088 → *Applications*: los 3 jobs en `SUCCEEDED`.

Para ver un job que falla a propósito:

```bash
docker compose exec client bash /workspace/jobs/run.sh fallar
```

📸 **Captura 6:** la terminal (4 intentos fallidos) y *Applications* con el job en `FAILED`.

## 3. Pruebas de fallos

Antes de cada prueba, revisa que todo esté bien:

```bash
docker compose exec client bash -c "hdfs dfsadmin -report | grep -E 'Live datanodes|Dead datanodes'"
```

Debe decir `Live datanodes (3)`. Si no, espera un poco.

### Prueba A: apagar un DataNode

```bash
docker compose exec client hdfs dfs -setrep -w 2 /reviews/input/reviews_filmaffinity.csv
docker compose exec client hdfs fsck /reviews/input -files -blocks -locations
```

📸 **A1:** el resultado del `fsck` (dónde está cada bloque).

```bash
docker compose stop datanode3
```

Espera ~1 minuto y recarga http://localhost:9870 → *Datanodes*.

📸 **A2:** datanode3 aparece como *Dead*.

```bash
docker compose exec client hdfs fsck /reviews/input -files -blocks -locations
docker compose exec client bash -c "hdfs dfs -cat /reviews/input/reviews_filmaffinity.csv | head -3"
```

📸 **A3:** el `fsck` (los bloques ya no usan datanode3, se copiaron a otro nodo) y el archivo leyéndose bien.

Dejarlo como estaba:

```bash
docker compose start datanode3
docker compose exec client hdfs dfs -setrep -w 3 /reviews/input/reviews_filmaffinity.csv
```

### Prueba B: apagar el NodeManager con un job corriendo

Usa dos terminales. En la **terminal 1**:

```bash
docker compose exec client bash /workspace/jobs/run.sh wordcount
```

Cuando salga `map 20%` aprox., en la **terminal 2**:

```bash
docker compose exec datanode2 yarn --daemon stop nodemanager
```

> Si el job termina antes de que alcances, repítelo.

📸 **B1:** terminal 1 con tareas `FAILED/KILLED` y luego el job terminando bien.
📸 **B2:** los contadores del final: `Launched map tasks` mayor que 11 y `Failed/Killed map tasks` mayor que 0.
📸 **B3:** (después de ~1 min) http://localhost:8088 → *Nodes*: datanode2 en `LOST`.

Dejarlo como estaba:

```bash
docker compose exec datanode2 yarn --daemon start nodemanager
```

### Prueba C: apagar una máquina completa

Terminal 1: `docker compose exec client bash /workspace/jobs/run.sh wordcount`
Terminal 2 (cuando salga `map 20%`): `docker compose kill datanode1`

📸 **C1:** el job termina bien y da el mismo resultado de siempre.
📸 **C2:** (después de ~1 min) http://localhost:9870 → *Datanodes*: datanode1 *Dead*.

Dejarlo como estaba:

```bash
docker compose start datanode1
```

### Prueba D: reiniciar el NameNode

Terminal 1:

```bash
docker compose exec client bash -c 'for i in $(seq 1 20); do hdfs dfs -put -f /workspace/datasets/reviews_filmaffinity.csv /tmp/copia$i.csv && echo "ok $i"; done'
```

Terminal 2 (a mitad de las subidas):

```bash
docker compose restart namenode
```

📸 **D1:** terminal 1 con los errores de conexión mientras el NameNode está caído.

```bash
docker compose exec client hdfs dfsadmin -safemode get
```

📸 **D2:** el modo seguro (`Safe mode is ON`, y luego `OFF` al repetir el comando).

Limpiar:

```bash
docker compose exec client bash -c "hdfs dfs -rm -skipTrash '/tmp/copia*.csv'"
```

## 4. Apagar todo al final

```bash
docker compose down
```

## Si algo falla

| Problema | Solución |
|---|---|
| Un nodo no vuelve | `docker compose start datanodeN` y esperar ~1 minuto |
| El job se queda en `ACCEPTED` | Falta un NodeManager apagado; volver a encenderlo (ver pruebas B y C) |
| Dice `safe mode` | Esperar unos segundos |
| Todo raro | `docker compose down -v`, luego `docker compose up -d` y repetir el paso 2 (borra lo que haya en HDFS) |
