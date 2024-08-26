CREATE TABLE IF NOT EXISTS working_data (
    data_ingestao DateTime,
    dado_linha String,
    tag String
) ENGINE = MergeTree()
ORDER BY data_ingestao

CREATE VIEW IF NOT EXISTS working_data_view_v2 AS
SELECT
    data_ingestao,
    JSONExtract(dado_linha, 'title', 'String') AS title,
    JSONExtract(dado_linha, 'releaseDate', 'String') AS releaseDate,
    toDateTime(JSONExtractInt(dado_linha, 'data_ingestao')) AS data_ingestao_datetime
FROM working_data