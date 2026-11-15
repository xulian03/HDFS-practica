# Hadoop en Docker

En este proyecto armamos un **mini clúster de Hadoop** en nuestro computador usando Docker, le pusimos a procesar un montón de reseñas de películas y después lo **sabotamos a propósito** (apagando nodos, matando contenedores) para ver si aguantaba.

Spoiler: casi todo sobrevive. Lo que no, también tiene su explicación.

📄 Las capturas y el paso a paso de cada prueba están en Rporte_HDFS.md.

---

## Antes de empezar: Debemos ocmprender los roles de las herramientas

- **Hadoop** es un conjunto de herramientas para guardar y procesar cantidades enormes de datos usando **varios computadores trabajando juntos** (un *clúster*), en lugar de uno solo muy potente.
- **HDFS** es la parte que guarda los archivos. Parte cada archivo en pedazos llamados **bloques** y guarda **varias copias** de cada uno en máquinas distintas. Así, si una máquina se daña, los datos no se pierden.
- **YARN** es el "jefe de obra": reparte el trabajo entre las máquinas.
- **MapReduce** es una forma de procesar datos en dos pasos: primero cada máquina trabaja su pedazo (*Map*) y luego se juntan los resultados (*Reduce*).
- **Docker** nos permite simular varias máquinas dentro de un solo computador, cada una en su propio "contenedor".

---

## Qué armamos

### El clúster

Levantamos todo con Docker Compose, usando **Hadoop 3.4.1**:

| Contenedor | Qué hace |
|---|---|
| `namenode` | El cerebro de HDFS: sabe en qué máquina está cada bloque |
| `datanode1`, `datanode2`, `datanode3` | Guardan los bloques y además ejecutan las tareas |
| `resourcemanager` | Reparte el trabajo en YARN |
| `historyserver` | Guarda el historial y los logs de los trabajos ya terminados |
| `client` | Desde acá lanzamos todos los comandos |

### Los datos

Usamos `reviews_filmaffinity.csv`: reseñas de películas de FilmAffinity (unos 11 MB). HDFS lo parte en **12 bloques** y los reparte entre los nodos.

### Los trabajos que corrimos

Se lanzan con el script `jobs/run.sh`:

| Trabajo | Qué hace |
|---|---|
| `wordcount` | Cuenta qué palabras se repiten más en las reseñas (la ganadora: `historia`, con 5081 apariciones) |
| `promedio` | Calcula la nota promedio de las películas según su género |
| `top` | Cuenta cuántas reseñas tiene cada película y se queda con las 10 que más tienen |
| `fallar` | Falla a propósito, para ver cómo se ve un trabajo con error y dónde se leen sus logs |


### Cómo verlo funcionando

Además de la terminal, el clúster tiene páginas web para ver qué está pasando:

| Qué muestra | Dirección |
|---|---|
| Estado de HDFS (nodos vivos o muertos) | http://localhost:9870 |
| Estado de YARN (nodos y trabajos) | http://localhost:8088 |
| Historial de trabajos | http://localhost:19888 |

### Cómo correrlo

```bash
docker compose up -d
docker compose exec client bash /workspace/jobs/run.sh setup      
docker compose exec client bash /workspace/jobs/run.sh wordcount  
```

---

## Las pruebas: ¿qué pasa si algo se rompe?

En cada prueba hicimos lo mismo: **provocar un fallo, ver cómo reacciona el clúster y restaurarlo**, siempre empezando con todo sano.

### Prueba A · Apagamos un DataNode

Apagamos `datanode3` y miramos qué hacía HDFS.

**Qué pasó:** el sistema lo marcó como caído, copió los bloques que estaban ahí a las máquinas que seguían vivas y el archivo **se pudo seguir leyendo sin problema**. No se perdió nada.

### Prueba B · Apagamos una máquina de cómputo en pleno trabajo

Pusimos a correr `wordcount` y, mientras trabajaba, detuvimos el NodeManager de `datanode2` (la parte que ejecuta tareas).

**Qué pasó:** YARN marcó esa máquina como perdida, mandó sus tareas a otras y el trabajo **terminó bien, con exactamente el mismo resultado**. Como los datos de esa máquina seguían intactos, lo único que se perdió fue poder de cómputo.

### Prueba C · Matamos una máquina entera

Pusimos a correr `promedio` y en plena fase Map matamos `datanode1`, o sea, se cayeron a la vez sus datos y su cómputo.

**Qué pasó:** el sistema intentó reconectarse varias veces, el avance del trabajo **retrocedió** (de 27 % a 0 %) porque las tareas se repitieron en otras máquinas, y al final terminó bien, aunque más lento: 1 min 46 s contra 21 s del trabajo sin fallos. Los datos se salvaron gracias a las copias.

### Prueba D · Reiniciamos el NameNode mientras se escribían archivos

Hicimos 20 escrituras seguidas en HDFS y reiniciamos el NameNode a la mitad.

**Qué pasó:** las primeras 5 salieron bien y después empezaron los errores de conexión. Al volver, el NameNode entró en *modo seguro* y rechazó escrituras hasta recuperar la información de los bloques. Mientras estuvo caído **no se pudo escribir nada**, aunque las otras tres máquinas seguían vivas. Lo ya escrito quedó intacto.

---

## Lo que aprendimos

- 🧱 **Las copias salvan los datos:** si una máquina se cae, siempre queda otra con el mismo bloque.
- 🔁 **El trabajo se reintenta solo:** si una máquina falla a mitad de un trabajo, otra lo retoma y el resultado sale igual.
- ⏱️ **Tolerar fallos tiene un costo:** con una máquina caída el trabajo tardó bastante más.
- 💥 **El NameNode es el punto débil:** si se cae, todo HDFS se frena. Por eso en sistemas reales se usan varios NameNode de respaldo.

---

## Si algo no funciona

| Problema | Qué hacer |
|---|---|
| Un nodo quedó caído y no vuelve | `docker compose start datanodeN` (y esperar ~1 minuto) |
| Un trabajo se queda en `ACCEPTED` | Seguramente falta restaurar un nodo de una prueba anterior |
| Sale `Name node is in safe mode` | Esperar unos segundos a que el NameNode termine de arrancar |
| Todo está raro | `docker compose down -v` y volver a levantar. ⚠️ Esto borra todo lo que haya en HDFS |
