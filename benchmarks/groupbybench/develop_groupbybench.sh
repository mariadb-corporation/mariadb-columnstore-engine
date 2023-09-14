#get the parent directory of this script
script_full_path=$(realpath $0)
script_dir_path=$(dirname $script_full_path)

cd $script_dir_path

hyperfine --prepare 'sync; echo 3 | sudo tee /proc/sys/vm/drop_caches' \
    'mariadb --database="bench" < query_bench_real.sql' --export-csv develop_bench_real.csv
hyperfine --prepare 'sync; echo 3 | sudo tee /proc/sys/vm/drop_caches' \
    'mariadb --database="bench" < query_two_groups.sql' --export-csv develop_bench_two_groups.csv
hyperfine --prepare 'sync; echo 3 | sudo tee /proc/sys/vm/drop_caches' \
    'mariadb --database="bench" < query_ten_groups.sql' --export-csv develop_bench_ten_groups.csv
