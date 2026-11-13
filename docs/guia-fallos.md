# Guía rápida: pruebas de tolerancia a fallos + capturas

Objetivo: ejecutar los 4 experimentos del punto 4 del taller **de corrido**, tomando las capturas justas (no más) para mostrárselas al profesor.
Las explicaciones largas están en `docs/sustentacion.md` (sección 5); aquí solo hay **qué ejecutar y qué capturar**.

Cada captura tiene un código (`A1`, `B3`…) que es también el nombre sugerido del archivo: `capturas/A1-fsck-antes.png`.

---

## 0. Preparación (una sola vez, 5 min)

**Herramientas**

- Captura de pantalla en Windows: `Win + Shift + S` → guardar en la carpeta `capturas/` con el código de esta guía.
- **Tres terminales** (T1, T2, T3) y **un navegador** con estas pestañas, en este orden:

| Pestaña | URL | Para qué |
|---|---|---|
| NN | http://localhost:9870/dfshealth.html#tab-datanode | DataNodes vivos / muertos |
| RM | http://localhost:8088/cluster/nodes | NodeManagers (RUNNING / LOST) |
| Apps | http://localhost:8088/cluster/apps | Jobs y sus intentos |
| Hist | http://localhost:19888 | Logs de jobs ya terminados |

**Comando de salud** (se usa antes y después de cada experimento). Cópialo en un bloc de notas:

```bash
docker compose exec client bash -c "hdfs dfsadmin -report | grep -E 'Live datanodes|Dead datanodes'; hdfs fsck / | grep -E 'Status|Under-replicated|Over-replicated'; yarn node -list -all 2>/dev/null"
```

Debe mostrar: `Live datanodes (3)`, `Dead datanodes (0)`, `Status: HEALTHY`, y 3 NodeManagers `RUNNING`.
**No empieces un experimento hasta que el clúster esté así.**

**Arranque limpio (si hace falta)**

```bash
docker compose up -d
docker compose exec client bash /workspace/jobs/run.sh setup
```

### Captura 0 — estado base (una vez)

| Código | Qué | Dónde | Debe verse |
|---|---|---|---|
| **0a** | Contenedores arriba | T1: `docker compose ps` | `namenode` y `resourcemanager` *healthy*; datanode1-3 *running* |
| **0b** | Salud inicial | T1: comando de salud | 3 Live, 0 Dead, HEALTHY, 3 RUNNING |
| **0c** | DataNodes en la web | pestaña **NN** | 3 DataNodes *In Service*, con *Last contact* de pocos segundos |
| **0d** | NodeManagers en la web | pestaña **RM** | 3 nodos *RUNNING* |

---

## Orden recomendado

**A → B → C → D.** Cada uno termina con su paso *Restaurar* y el comando de salud: no se mezclan entre sí.
Tiempo total: ~15 min si todo sale a la primera.

---

## A. Apagar un DataNode → re-replicación (~3 min)

**Idea para decir:** el NameNode detecta al nodo muerto (~1 min sin *heartbeats*), ve bloques con menos copias de las pedidas y los copia a otro nodo. El archivo se sigue leyendo sin enterarse.

**Preparar** (con 3 nodos y replicación 3 no hay a dónde re-replicar, así que se baja a 2):

```bash
docker compose exec client hdfs dfs -setrep -w 2 /reviews/input/reviews_filmaffinity.csv
docker compose exec client hdfs fsck /reviews/input -files -blocks -locations
```

📸 **A1 · Antes.** T1: salida del `fsck`. Debe verse cada bloque con **2 réplicas** y sus ubicaciones (`DatanodeInfoWithStorage[...]`). Anota en qué nodos está el bloque 1.

**Provocar el fallo** (T1) y observar (T2, T3):

```bash
# T1
docker compose stop datanode3
# T2: se actualiza solo cada 2 s
docker compose exec client watch -n2 "hdfs dfsadmin -report | grep -E 'Live datanodes|Dead datanodes'"
# T3: se actualiza solo cada 2 s
docker compose exec client watch -n2 "hdfs fsck /reviews/input | grep -E 'Status|Under-replicated|Over-replicated'"
```

| Código | Cuándo | Dónde | Debe verse |
|---|---|---|---|
| 📸 **A2** | 0-60 s | pestaña **NN** (recargar) | datanode3 con *Last contact* creciendo (10 s, 30 s…): todavía no sabe que murió |
| 📸 **A3** | ~1 min | T2 **y** pestaña **NN** | `Live datanodes (2)` · `Dead datanodes (1)`; en la web datanode3 pasa a *Dead* |
| 📸 **A4** | justo después | T3 | `Under-replicated blocks: N` (N > 0). Dura pocos segundos: deja el `watch` de T3 corriendo para no perderlo |
| 📸 **A5** | segundos después | T3 | `Under-replicated blocks: 0`: **los bloques volvieron a tener 2 copias** (ahora en datanode1 y 2) |

Con el nodo caído, demostrar que se sigue leyendo:

```bash
docker compose exec client bash -c "hdfs dfs -cat /reviews/input/reviews_filmaffinity.csv | head -3"
docker compose exec client hdfs fsck /reviews/input -files -blocks -locations
```

📸 **A6 · Lectura sin problema + ubicaciones nuevas.** T1: las 3 líneas leídas y el `fsck` con los bloques ahora solo en datanode1 y datanode2.

> A5 y A6 son la prueba central: `A1` (nodos originales) → `A6` (nodos nuevos) muestra la re-replicación.

**Restaurar** (T1; cierra T2 y T3 con `Ctrl+C`):

```bash
docker compose start datanode3
docker compose exec client hdfs dfs -setrep -w 3 /reviews/input/reviews_filmaffinity.csv
```

📸 **A7 · Recuperado.** T1: comando de salud → `Live datanodes (3)`, HEALTHY. (Puede aparecer *Over-replicated* unos segundos: datanode3 vuelve con bloques viejos y el NameNode borra los sobrantes. Espera a que desaparezca.)

---

## B. Detener un NodeManager mientras corre un job (~3 min)

**Idea para decir:** solo cae el *cómputo* de datanode2 (el DataNode sigue vivo). YARN reintenta las tareas en otros nodos y el job termina bien.

**Preparar:** deja escrito en T2 (sin Enter) el comando de parada. Tienes pocos segundos entre que el job arranca y termina.

```bash
# T2 (escrito, sin ejecutar)
docker compose exec datanode2 yarn --daemon stop nodemanager
```

```bash
# T1
docker compose exec client bash /workspace/jobs/run.sh wordcount
```

Cuando T1 muestre `map 10%`–`map 30%`: **Enter en T2**.

> Si el job termina antes de que alcances: es normal, repite (`run.sh` borra la salida anterior). Aprieta Enter en cuanto aparezca `Running job`.

| Código | Cuándo | Dónde | Debe verse |
|---|---|---|---|
| 📸 **B1** | al ejecutar T2 | T1 | Mensajes de tareas `FAILED`/`KILLED` (`attempt_..._m_00000X_0`) y reintentos |
| 📸 **B2** | al terminar el job | T1 (final) | `map 100% reduce 100%` y `Job ... completed successfully` + counters |
| 📸 **B3** | al terminar | T1 (counters) | `Launched map tasks` **mayor que 11**, y `Failed map tasks` o `Killed map tasks` **> 0** |
| 📸 **B4** | ≥ 60 s después | pestaña **RM** | datanode2 en estado **LOST** (los otros dos RUNNING) |
| 📸 **B5** | al terminar | pestaña **Apps** → clic en el job | `FINISHED` / `SUCCEEDED` |
| 📸 **B6** | al terminar | Apps → job → *Tracking UI* / *Attempts* | Intentos `_0` fallidos y `_1` exitosos en **otro nodo**. Si el ApplicationMaster estaba en datanode2 verás además `appattempt_..._000002` |

También sirve: `docker compose exec client yarn node -list -all` → 📸 **B7**: datanode2 `LOST`, los otros `RUNNING`.

Comprobar que el resultado no cambió (el top 15 debe ser el mismo de siempre: `historia 5081`, `mejor 3932`…):

📸 **B8** · T1: las líneas del *Top 15* que imprime `run.sh` al final.

**Restaurar:**

```bash
docker compose exec datanode2 yarn --daemon start nodemanager
```

📸 **B9 · Recuperado.** Pestaña **RM** (esperar ~15 s y recargar): datanode2 vuelve a *RUNNING*.

---

## C. Matar una máquina entera durante el Map (~3 min)

**Idea para decir:** caen a la vez un DataNode *y* un NodeManager. Las lecturas van a otra réplica y las tareas se reintentan: HDFS + YARN juntos, y el resultado sigue igual.

```bash
# T1
docker compose exec client bash /workspace/jobs/run.sh wordcount
# T2 (escrito, Enter cuando T1 muestre "map 20%" aprox.)
docker compose kill datanode1
```

| Código | Cuándo | Dónde | Debe verse |
|---|---|---|---|
| 📸 **C1** | durante el job | T1 | Errores/reintentos de tareas y el job sigue avanzando |
| 📸 **C2** | al terminar | T1 | `completed successfully` + Top 15 **idéntico** + counters con `Launched map tasks` > 11 |
| 📸 **C3** | ~1 min | pestaña **NN** | datanode1 *Dead* |
| 📸 **C4** | ~1 min | pestaña **RM** | datanode1 *LOST* |
| 📸 **C5** | con el nodo caído | T1: `docker compose exec client bash -c "hdfs fsck / \| grep -E 'Status\|Under-replicated'"` | Bloques *Under-replicated* (replicación 3 con solo 2 nodos vivos), pero **sin bloques perdidos** |

> `Data-local map tasks` sigue alto porque con replicación 3 todos los nodos tienen todos los bloques.

**Restaurar:**

```bash
docker compose start datanode1
```

(el contenedor levanta DataNode **y** NodeManager solo). Esperar ~1 min y correr el comando de salud.

📸 **C6 · Recuperado.** Comando de salud: 3 Live, HEALTHY, 3 RUNNING.

---

## D. Reiniciar el NameNode durante escrituras (~2 min)

**Idea para decir:** perder **un** DataNode no detiene nada; perder **el** NameNode detiene todo HDFS (punto único de fallo). Al volver arranca en *safe mode* hasta que los DataNodes le reportan sus bloques.

```bash
# T1: 20 escrituras seguidas
docker compose exec client bash -c 'for i in $(seq 1 20); do hdfs dfs -put -f /workspace/datasets/reviews_filmaffinity.csv /tmp/copia$i.csv && echo "ok $i"; done'
# T2 (Enter a mitad de la subida, p. ej. después de "ok 5")
docker compose restart namenode
```

| Código | Cuándo | Dónde | Debe verse |
|---|---|---|---|
| 📸 **D1** | durante el reinicio | T1 | Líneas `ok N` y luego errores de conexión (`Connection refused` / `Retrying connect`) |
| 📸 **D2** | justo tras volver | pestaña **NN** (:9870 Overview) | Aviso *"Safe mode is ON..."* (si ya salió, es muy rápido: ver D3) |
| 📸 **D3** | justo tras volver | T3: `docker compose exec client hdfs dfsadmin -safemode get` | `Safe mode is ON` y luego `Safe mode is OFF` (repetir el comando) |
| 📸 **D4** | al terminar | T1 | Las escrituras vuelven a decir `ok N` (o fallaron las que cayeron en la ventana; es lo esperado) |

Verificar que no se perdió nada:

```bash
docker compose exec client bash -c "hdfs dfs -ls /tmp | head; hdfs fsck / | grep Status"
```

📸 **D5 · Datos intactos.** T1: archivos `/tmp/copia*` y `Status: HEALTHY`.

**Limpiar:**

```bash
docker compose exec client bash -c "hdfs dfs -rm -skipTrash '/tmp/copia*.csv'"
```

---

## Bonus (punto 3): un job FAILED a propósito (1 min)

El taller pide ver los estados RUNNING, FAILED y SUCCEEDED. SUCCEEDED ya lo tienes (B5).

```bash
docker compose exec client bash /workspace/jobs/run.sh fallar
```

| Código | Dónde | Debe verse |
|---|---|---|
| 📸 **F1** | T1 | 4 intentos fallidos por tarea (`_0` … `_3`) y `Job failed` |
| 📸 **F2** | pestaña **Apps** | El job en `FINISHED` / `FAILED` |
| 📸 **F3** | `docker compose exec client bash -c "yarn logs -applicationId <id> \| grep -m1 -A6 Traceback"` (el `<id>` sale en T1 o en Apps) | El `Traceback` que explica **por qué** falló |

---

## Tabla resumen para el profesor

Sugerencia: pegar las capturas en un documento con este orden y un pie de una línea por cada una.

| Experimento | Capturas clave (las mínimas) | Qué demuestran |
|---|---|---|
| **Base** | 0b, 0c, 0d | Clúster sano: 3 DataNodes, 3 NodeManagers |
| **A · DataNode** | A1, A3, A5, A6, A7 | Detección (Dead), re-replicación, lectura sin interrupción, recuperación |
| **B · NodeManager** | B1, B3, B4, B6, B8 | Reintentos de tareas, nodo LOST, resultado idéntico |
| **C · Máquina entera** | C2, C3, C4, C5, C6 | Fallo HDFS + YARN a la vez, sin pérdida de datos |
| **D · NameNode** | D1, D3, D5 | Punto único de fallo, safe mode, datos intactos |
| **Punto 3** | B5, F1, F2, F3 | Estados SUCCEEDED y FAILED, y lectura de logs |

---

## Si algo sale mal

| Problema | Qué hacer |
|---|---|
| Un nodo quedó `Dead`/`LOST` y no vuelve | `docker compose start datanodeN` y/o `docker compose exec datanodeN yarn --daemon start nodemanager`; esperar ~1 min |
| El job se queda en `ACCEPTED` | Falta un NodeManager (de un experimento anterior). Revisa la pestaña **RM** y restáuralo |
| `Name node is in safe mode` | Esperar unos segundos o `docker compose exec client hdfs dfsadmin -safemode leave` |
| Todo está raro | `docker compose down -v` · `docker compose up -d` · `docker compose exec client bash /workspace/jobs/run.sh setup` (⚠️ `-v` borra todo lo que hay en HDFS) |
