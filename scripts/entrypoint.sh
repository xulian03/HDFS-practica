#!/usr/bin/env bash
set -euo pipefail

role="${1:-}"

case "$role" in
  namenode)
    if [[ ! -f /data/name/current/VERSION ]]; then
      if [[ -n "$(find /data/name -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
        echo "NameNode directory is not empty but has no VERSION file. Refusing to format." >&2
        exit 1
      fi
      hdfs namenode -format -nonInteractive
    fi
    exec hdfs namenode
    ;;
  datanode)
    # Cada "máquina obrera" corre dos procesos: el NodeManager (YARN) en segundo plano
    # y el DataNode (HDFS) como proceso principal. Así YARN puede lanzar los mappers en el
    # mismo host que tiene los bloques (data locality).
    # - docker stop datanodeN                                   -> se cae la máquina entera
    # - docker compose exec datanodeN yarn --daemon stop nodemanager -> solo se cae YARN en ese nodo
    yarn --daemon start nodemanager
    exec hdfs datanode
    ;;
  resourcemanager)
    exec yarn resourcemanager
    ;;
  historyserver)
    exec mapred historyserver
    ;;
  client)
    exec tail -f /dev/null
    ;;
  *)
    exec "$@"
    ;;
esac
