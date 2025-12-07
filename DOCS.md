# VUT FIT - PDI 25/26

### Timotej Halenár, Lucie Jadrná

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

3. `edits_by_type`

`edits_by_type` Zoskupuje všetky zmeny podľa typu (edit, new, log, …) po odfiltrovaní botov a zoradí ich podľa počtu výskytov.

4. `user_counts_per_minute`

`user_counts_per_minute` Počíta, koľko úprav vykonal každý používateľ za jednotlivé minútové okná, pričom ignoruje botov a neznámych používateľov.

5. `avg_edit_length_change`

`avg_edit_length_change` Vypočítava priemernú zmenu dĺžky editácie počas posuvných dvojminútových okien s 30-sekundovým posunom.

## Ukladanie výsledkov/prezentácia

Dáta sú ukladané do JSON súborov za využitia funkcionality Apache Spark `writeStream`. Zapisované sú dáta aj checkpointy.

## Spustenie

Podrobný postup spúšťania aplikácie je v súbore `INSTALL.md`. Pri spustení pomocou `docker-compose up` sú vytvorené Kafka broker, producer, a Spark consumer. Ako východzí dotaz je `--server-counts` a `--limit` je `30`, čo znamená, že stream je spracovávaný 30 sekúnd, a potom skončí. Po dobehnutí je možné spustiť ďalšie dotazy v novom terminálovom okne za použitia `docker-compose run consumer [ARGS]`, kde je možné zadať ľubovoľné kombinácie dotazov a špecifikovať limit. Pri nešpecifikovanom limite bude dotaz bežať až do prerušenia.

Priebeh Spark úloh je možné sledovať vo webovom UI na `localhost:4040`.

## Príklady spustenia aplikácie

Prerekvizita:

```
$ docker-compose up
```

Spustenie dotazu `edits-per-minute` bez časového obmedzenia:

```
$ docker-compose run consumer --edits-per-minute
```

Výsledky sú v `consumer_output/edits-per-minute`.

Spustenie dotazu `server-counts` na 60 sekúnd:

```
$ docker-compose run consumer --server-counts --limit 60
```

Výsledky sú v `consumer_output/server-counts`.

Spustenie `server-counts` a `edits-per-minute` na 60 sekúnd naraz:

```
$ docker-compose run consumer --server-counts ---edits-per-minute --limit 60
```

Výsledky sú v `consumer_output/edits-per-minute` a `consumer_output/server-counts`.
