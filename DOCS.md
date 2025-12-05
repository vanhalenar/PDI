# VUT FIT - PDI 25/26
# Apache Spark Structured Streaming
# Projektová dokumentácia

V tomto projekte využívame Apache Spark na spracovanie prúdu dát. Používame Python `pyspark` modul.

## Zdroj prúdových dát

Využívame prúd dát `https://stream.wikimedia.org/v2/stream/recentchange`. V tomto prúde sú obsiahnuté úpravy wikimedia dát v reálnom čase. Dáta sú streamované pomocou HTTP SSE.

## Spôsob čítania dát

Na zachytávanie prúdu využívame Apache Kafka. Implementácia je v súbore `sse_to_kafka.py`. Za použitia `requests` a `sseclient-py` zachytávame stream, kde každý event posielame Kafka brokeru. Udalosti sú uložené v topicu `sse-topic`. Takisto implementujeme reconnect loop v prípade, že je stream prerušený. 

Na Kafku je napojený konzument dát `pyspark_consumer.py`, ktorý číta prúd dát pomocou `spark.readStream`, kde sú dáta následne spracované.

## Dotazy
0. `capture-all` (DEBUG)

`capture-all` zachytáva všetky dáta, ktoré stream posiela. Využívali sme to pri ladení, na pochopenie schémy dát, apod.

1. `server-counts`

`server-counts` dáta zoskupuje podľa najčastejšie upravovaného servera (commons.wikimedia.org, www.wikidata.org, en.wikipedia.org, ...) a výsledky zoradí podľa najčastejšie upravovaného servera.

2. `edits-per-minute`

`edits-per-minute` vypočítava úpravy za minútu za využitia tumbling window. 

3.

4.

5.

## Ukladanie výsledkov/prezentácia

Dáta sú ukladané do JSON súborov za využitia funkcionality Apache Spark `writeStream`. Zapisované sú dáta aj checkpointy.

## Ukážky behu programu

Postup spúšťania aplikácie je v súbore `INSTALL.md`. Ukážky behu a popis offline testov, sú v súbore `TESTING.md`.