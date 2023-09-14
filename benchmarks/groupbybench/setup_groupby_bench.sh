#get the parent directory of this script
script_full_path=$(realpath $0)
script_dir_path=$(dirname $script_full_path)

mariadb < "$script_dir_path/create_bench_database.sql"

cpimport -s ',' bench bench_real "$script_dir_path/bench_data/dump_11_08_22_36kk.csv"
cpimport -s ',' bench bench_two_groups "$script_dir_path/bench_data/two_groups.csv"
cpimport -s ',' bench bench_ten_groups "$script_dir_path/bench_data/ten_groups.csv"