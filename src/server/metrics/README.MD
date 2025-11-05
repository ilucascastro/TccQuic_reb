Métricas do Servidor — Arquivos CSV (versão focada)

Este servidor agora produz quatro CSVs, todos do lado servidor:

1) reqlog.csv — por requisição (tempos e status)
   Columns: time_ns,event,class,segment,tile,bytes,ontime,drop,qd_ms,svc_ms,rsp_ms

2) class_agg.csv — agregado por classe (apenas métricas do PDF)
   Columns: ts,class,event,completed,dropped_deadline,bytes_sent,bytes_on_time,
            avg_queue_delay_ms,avg_service_time_ms,avg_response_time_ms,
            ontime_ratio_pct,bytes_on_time_ratio_pct,avg_slack_ms,avg_time_to_drop_ms

3) queue_len.csv — amostras de tamanho de fila por classe (se o scheduler expor)
   Columns: ts,class,queue_len

4) server_summary.csv — resumo final (shares, Jain, throughput, contadores)
   Columns: ts_start,ts_end,duration_s,
            bytes_low,bytes_med,bytes_high,
            throughput_low_kbps,throughput_med_kbps,throughput_high_kbps,
            class_share_low_pct,class_share_med_pct,class_share_high_pct,
            jain_fairness,
            drop_rate_low_pct,drop_rate_med_pct,drop_rate_high_pct,
            preemptions,inversions,
            work_conserving_ratio_pct,
            stale_bytes

Como funciona
- stream_handler.go:
  - cria reqlog.csv, inicializa class_agg.csv, queue_len.csv e server_summary.csv
  - marca início (MarkRunStart) e escreve resumo ao final (WriteSummaryAndClose)
  - registra ENQUEUE/START/COMPLETE/DROP por request
  - em drop por deadline, estima stale_bytes via os.Stat() do arquivo do tile
- metrics.go:
  - contadores por classe e globais (preemptions, inversions, in-service)
  - work-conserving via amostras de fila + in-service
  - grava class_agg (a cada complete/drop)
  - grava queue_len (OnQueueSample)
  - grava server_summary no final (Class Share, Jain, Throughput, Drop Rate, etc.)

Requisito para queue_len.csv e work-conserving
- Se o TaskScheduler expor:
    type QueueLenProvider interface {
        QueueLenPerClass() map[model.Priority]int
    }
  o stream_handler vai amostrar a cada 100ms e chamar metrics.M().OnQueueSample(...).
  Se não expor, queue_len.csv fica desabilitado e o work-conserving fica menos preciso.

Hooks opcionais no escalonador
- Preempção: chame metrics.M().OnPreempt(preemptedClass, preemptorClass)
- Amostra de fila (push): chame metrics.M().OnQueueSample(currentLens)

Diretório de saída (no host Mininet)
- /tmp/server_scheduler_test/