Autor: Mikołaj Błaż
Album: 346862
14.12.2016

W katalogu 'scripts' znajdują się skrypty, w celu pełnego wystartowania hadoopa
(łącznie z pobraniem) należy uruchomić skrypt 'run_all.sh' z argumentami 'master slaves'.
Kopiowanie do HDFS - put_to_hdfs.sh
Kompilacja i uruchomienie - compilenrun.sh

Sliding Aggregation:
Klasa SlidingAggregation implementuje wszystkie kroki algorytmu.
Parametry wywołania:
-D my.threshold=0.1         ustawia prawdopodobieństwo wzięcia do próbki na 0.1
-D my.window=50             ustawia rozmiar okna agregacji na 50
-D my.reducers=2            ustawia liczbę ReduceTasków na 4
-D mapred.reduce.tasks=2    ustawia liczbę ReduceProcesów na 4

Następnie muszą wystąpić 2 argumenty: plik wejściowy oraz katalog wyjściowy



Przykładowe wykonanie (zakładamy, że hadoop jest uruchomiony):
Z katalogu src:

# Umieszczenie plików wejścia w HDFS
../scripts/put_to_hdfs.sh ../input/tosort100.txt
# Kompilacja i wykonanie
../scripts/compilenrun.sh SlidingAggregation -D threshold=0.1 -D my.window=37 -D my.reducers=2 -D mapred.reduce.tasks=2 /input/tosort100.txt output

