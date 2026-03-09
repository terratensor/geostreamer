# Анализ testdata/test.csv (пропускаем заголовок)
tail -n +2 testdata/test.csv | cut -d'|' -f1 | sort | uniq -c | awk '
BEGIN {
    min = 999999
    max = 0
    sum = 0
    count = 0
}
{
    if ($1 < min) min = $1
    if ($1 > max) max = $1
    sum += $1
    count++
}
END {
    printf "Всего doc_id: %d\n", count
    printf "Всего записей: %d\n", sum
    printf "Среднее: %.2f записей на doc_id\n", sum/count
    printf "Минимум: %d\n", min
    printf "Максимум: %d\n", max
    printf "Медианное распределение:\n"
}'

# Для распределения по перцентилям
tail -n +2 testdata/test.csv | cut -d'|' -f1 | sort | uniq -c | awk '{print $1}' | sort -n | awk '
BEGIN {
    lines[0] = 0
    i = 0
}
{
    lines[i++] = $1
}
END {
    n = i
    printf "10-й перцентиль: %d\n", lines[int(n*0.1)]
    printf "25-й перцентиль: %d\n", lines[int(n*0.25)]
    printf "50-й перцентиль (медиана): %d\n", lines[int(n*0.5)]
    printf "75-й перцентиль: %d\n", lines[int(n*0.75)]
    printf "90-й перцентиль: %d\n", lines[int(n*0.9)]
    printf "95-й перцентиль: %d\n", lines[int(n*0.95)]
    printf "99-й перцентиль: %d\n", lines[int(n*0.99)]
}'