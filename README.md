# HDFS, YARN y MapReduce con Docker

Clúster Hadoop 3.4.1 local (NameNode, ResourceManager, HistoryServer, 3 DataNodes con NodeManager y un cliente) y tres jobs MapReduce en Python (Hadoop Streaming) sobre reseñas de FilmAffinity.

```bash
docker compose build
docker compose up -d
docker compose exec client bash /workspace/jobs/run.sh setup      # sube el dataset a HDFS
docker compose exec client bash /workspace/jobs/run.sh todos      # wordcount, promedio y top
```

| Documento | Contenido |
|---|---|
| [`docs/sustentacion.md`](docs/sustentacion.md) | Guion de la sustentación: arquitectura, jobs, verificación y tolerancia a fallos, con resultados esperados |
| [`docs/guia-fallos.md`](docs/guia-fallos.md) | Guía rápida para ejecutar los 4 fallos y qué capturas tomar |

UIs: NameNode http://localhost:9870 · ResourceManager http://localhost:8088 · HistoryServer http://localhost:19888
