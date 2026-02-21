

## Fonte de dados (sem scraping)

Este projeto usa a **API pública do CNPJá** (`https://open.cnpja.com/office/:cnpj`) para obter Razão Social, Simples Nacional e SIMEI, evitando bloqueios de scraping.

> A API pública é limitada a **5 consultas por minuto por IP** — use **delay de 12.5s ou maior**.

## Cache + Resume (mais rápido sem risco)

O backend mantém um **cache local em SQLite** para:

- Reaproveitar resultados de CNPJs já consultados (execuções futuras ficam muito mais rápidas)
- Permitir **retomar** um lote interrompido (se cair internet/PC, ao rodar de novo ele usa o cache e continua)

### Onde fica o cache

Por padrão, o arquivo é criado em:

- `./data/cnpja_cache.sqlite3`

### Variáveis de ambiente (opcionais)

- `CNPJA_CACHE_PATH` (padrão: `data/cnpja_cache.sqlite3`)
- `CNPJA_CACHE_TTL_SECONDS` (padrão: `86400` = 24h)

### Observação

Mesmo com cache, o limite de 5/min continua valendo **para CNPJs novos**. Para cache hit, o processamento não espera.
