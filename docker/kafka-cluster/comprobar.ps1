param([switch]$EnVivo, [switch]$ConCaida)

$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    function Invoke-Compose {
        $output = & docker compose @args
        if ($LASTEXITCODE -ne 0) { throw "docker compose $args falló (código $LASTEXITCODE)" }
        $output
    }

    $config = Invoke-Compose config --format json | ConvertFrom-Json
    $names = $config.services.PSObject.Properties.Name
    if (@($names -match '^zk[1-3]$').Count -ne 3 -or
        @($names -match '^broker[1-3]$').Count -ne 3 -or 'kafka-ui' -notin $names) {
        throw 'La topología requiere 3 ZooKeepers, 3 brokers y Kafka UI.'
    }
    'Compose: 3 ZooKeepers, 3 brokers y Kafka UI'
    if (-not ($EnVivo -or $ConCaida)) { return }

    $bootstrap = 'broker1:29092,broker2:29092'
    $topics = @('p360.interacciones.v1', 'p360.pagos.v1')
    Invoke-Compose run --rm init-kafka
    foreach ($topic in $topics) {
        $describe = Invoke-Compose exec -T broker1 kafka-topics --bootstrap-server $bootstrap --describe --topic $topic
        if (($describe -join "`n") -notmatch 'PartitionCount:\s*3\s+ReplicationFactor:\s*3' -or
            @($describe -match 'Isr:\s*\d+,\d+,\d+').Count -ne 3 -or
            ($describe -join "`n") -notmatch 'min.insync.replicas=2') {
            throw "Política o ISR inesperado para $topic : $describe"
        }
        $describe
    }
    $ui = Invoke-RestMethod 'http://127.0.0.1:8091/api/clusters'
    if (-not ($ui | Where-Object { $_.name -eq 'pedidos360' -and $_.brokerCount -eq 3 })) {
        throw 'Kafka UI no reportó los tres brokers.'
    }
    'Kafka UI: reporta 3 brokers'

    if ($ConCaida) {
        # Topic desechable: la prueba no introduce eventos falsos en los flujos de negocio.
        $testTopic = 'p360.demo.' + [Guid]::NewGuid().ToString('N')
        Invoke-Compose exec -T broker1 kafka-topics --bootstrap-server $bootstrap --create --topic $testTopic --partitions 3 --replication-factor 3 --config min.insync.replicas=2
        try {
            Invoke-Compose stop broker3
            $deadline = [DateTime]::UtcNow.AddSeconds(45)
            do {
                $describe = Invoke-Compose exec -T broker1 kafka-topics --bootstrap-server $bootstrap --describe --topic $testTopic
                $ready = @($describe -match 'Isr:\s*(?:1,2|2,1)(?:\s|$)').Count -eq 3
                if (-not $ready) { Start-Sleep -Seconds 1 }
            } until ($ready -or [DateTime]::UtcNow -gt $deadline)
            if (-not $ready) { throw 'ISR no bajó a 2 antes del timeout.' }
            $marker = 'prueba-rf3'
            $marker | & docker compose exec -T broker1 kafka-console-producer --bootstrap-server $bootstrap --topic $testTopic --producer-property acks=all --producer-property request.timeout.ms=3000 --producer-property delivery.timeout.ms=10000
            if ($LASTEXITCODE -ne 0) { throw 'No se pudo producir con un broker detenido.' }
            $read = Invoke-Compose exec -T broker1 kafka-console-consumer --bootstrap-server $bootstrap --topic $testTopic --from-beginning --max-messages 1 --timeout-ms 10000
            if ($marker -notin $read) { throw 'No se recuperó el mensaje producido con dos réplicas.' }
            'Tolerancia: ISR 2, escritura acks=all y lectura correctas'
        } finally {
            Invoke-Compose start broker3
            Invoke-Compose exec -T broker1 kafka-topics --bootstrap-server $bootstrap --delete --topic $testTopic
        }
    }
} finally {
    Pop-Location
}
