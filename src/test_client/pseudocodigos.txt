// Pseudocódigo para o Algoritmo de Agendamento e Seleção de Bitrate Adaptativo

// Definições de Constantes e Tipos
CONSTANTE PRIORIDADE_ALTA = "ALTA"
CONSTANTE PRIORIDADE_MEDIA = "MEDIA"
CONSTANTE PRIORIDADE_BAIXA = "BAIXA" 
CONSTANTE BITRATE_ALTO
CONSTANTE BITRATE_MEDIO
CONSTANTE BITRATE_BAIXO
CONSTANTE LIMITE_BUFFER_MINIMO
CONSTANTE LIMITE_BUFFER_MAXIMO

ESTRUTURA EntradaTabelaTaxa:
    bitrate: NUMERICO
    threshold: NUMERICO

// Tabela de Bitrates (ordenada do maior para o menor bitrate)
LISTA_ORDENADA TABELA_DE_TAXAS:
    - (BITRATE_ALTO, THRESHOLD_ALTO)
    - (BITRATE_MEDIO, THRESHOLD_MEDIO)
    - (BITRATE_BAIXO, THRESHOLD_BAIXO)

// ----------------------------------------------------------------------------------------------------

FUNÇÃO AgendarSegmento(idSegmento, prazoFinal, vazaoMedia, nivelBuffer, primeiroTile, ultimoTile, rastroFOV):
    // Itera sobre todos os tiles dentro do segmento especificado.
    PARA CADA tileID DE primeiroTile ATÉ ultimoTile:
        // Verifica se o tile está dentro do Campo de Visão (FOV).
        // A função rastroFOV.Contem(idSegmento, tileID) é assumida para verificar a presença no FOV.
        EM_FOV <- (rastroFOV EXISTE) E rastroFOV.Contem(idSegmento, tileID)

        // Atribui prioridade com base na visibilidade do tile no FOV.
        SE EM_FOV ENTÃO:
            prioridade <- PRIORIDADE_ALTA
        SENÃO:
            prioridade <- PRIORIDADE_BAIXA

        // Seleciona o bitrate adequado para o tile.
        // Assume-se que 'selecionarBitrate' é uma função auxiliar para decidir o bitrate.
        bitrateRequisitado <- SelecionarBitrate(vazaoMedia, nivelBuffer, EM_FOV)

        // Adquire um recurso do semáforo para controle de concorrência.
        SEMAFORO.Adquirir()
        // Incrementa o contador do WaitGroup para aguardar a conclusão das tarefas.
        CONTADOR_TAREFAS.Adicionar(1)
        // Inicia uma execução paralela para processar o tile.
        EXECUTAR_PARALELO ProcessarTile(idSegmento, tileID, prazoFinal, bitrateRequisitado, prioridade, EM_FOV)

    // O WaitGroup (CONTADOR_TAREFAS) é assumido para ser aguardado em um ponto posterior,
    // garantindo que todos os tiles sejam processados antes de continuar.

// ----------------------------------------------------------------------------------------------------

FUNÇÃO SelecionarBitrate(vazaoMedia, bufferAtual, tileEstaNoFOV):
    // Se o tile não estiver no FOV, o bitrate é fixado no mínimo para economizar recursos.
    SE tileEstaNoFOV == FALSO ENTÃO:
        RETORNAR BITRATE_BAIXO

    // Verifica o nível do buffer para ajustar o bitrate.
    SE bufferAtual < LIMITE_BUFFER_MINIMO ENTÃO:
        LOG("Nível do buffer abaixo do mínimo; forçando BITRATE_BAIXO.")
        RETORNAR BITRATE_BAIXO

    // A lógica para 'bufferAtual > LIMITE_BUFFER_MAXIMO' pode ser adicionada aqui se necessário,
    // mas no momento, a tabela de taxas será seguida.

    // Itera sobre a tabela de taxas para encontrar o bitrate mais adequado com base na vazão média.
    PARA CADA (bitrate, threshold) EM TABELA_DE_TAXAS:
        SE vazaoMedia >= threshold ENTÃO:
            // Retorna o primeiro bitrate cujo threshold é satisfeito, que é o mais agressivo possível.
            RETORNAR bitrate

    // Fallback defensivo: se nenhum limiar for atendido, retorna o bitrate mínimo.
    RETORNAR BITRATE_BAIXO

// ----------------------------------------------------------------------------------------------------

FUNÇÃO ProcessarTile(idSegmento, tileID, prazoFinal, bitrate, prioridade, emFOV):
    // Este é um espaço reservado para a lógica real de processamento de um tile.
    // Pode incluir:
    // - Requisição do tile com o bitrate e prioridade especificados.
    // - Lidar com o prazo final (deadline).
    // - Gerenciamento de erros e retransmissões.
    // - Notificação de conclusão ao semáforo e WaitGroup.

    LOG("Processando Tile " + tileID + " do Segmento " + idSegmento + " com Bitrate " + bitrate + " e Prioridade " + prioridade + ". Em FOV: " + emFOV)

    // Exemplo de liberação do semáforo e conclusão da tarefa.
    SEMAFORO.Liberar()
    CONTADOR_TAREFAS.Concluir()