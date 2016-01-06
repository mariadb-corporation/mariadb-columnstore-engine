usage()
{
  echo "Usage: ${1##*/} [-t n]"
  echo "Options:"
  echo "  -h    display this help"
  echo "  -t    number of tables, default is 5"
}

tables=5

while [[ "$1" != "" ]]
do
  case $1 in
    -h)   usage $0; exit;;
    -t1)  tables=1;;
    -t2)  tables=2;;
    -t3)  tables=3;;
    -t4)  tables=4;;
    -t5)  tables=5;;
    -t)   shift; if [ "$1" != "" ]; then tables=$1; fi;;
    *)    echo "unknow option"; exit;;
  esac
  shift
done

if [ $tables -lt 1 ]; then
  echo "Invalid table number -- $tables";
  exit
elif [ $tables -gt 5 ]; then
  echo "$tables tables will generate millions of queries";
  exit
fi


jt=(left inner right)
tn=(t1 t2 t3 t4 t5)

if [ $tables -eq 1 ]; then
  for t in {1..5}; do
    echo "select * from t$t;"
  done
elif [ $tables -eq 2 ]; then
  # table id
  for t1 in 0 1; do
    for t2 in 0 1 ; do
      if [ $t1 = $t2 ]
      then
        continue
      fi

      t=($t1 $t2)

      # join type
      for j1 in 0 1 2; do
        for j2 in 0 1 2; do
          for j3 in 0 1 2; do

            # join table and column
            [ ${t[1]} -gt ${t[0]} ] && cn1=${t[1]} || cn1=${t[0]}

            echo -n "select t1.*, t2.* from" ${tn[${t[0]}]} ${jt[$j1]}
            echo -n " join" ${tn[${t[1]}]}
            echo -n " on" ${tn[${t[0]}]}.c$cn1 = ${tn[${t[1]}]}.c$cn1
            echo    " order by 1,2,3,4,5,6,7,8,9,10,11,12;"
          done
        done
      done
    done
  done
elif [ $tables -eq 3 ]; then
  # table id
  for t1 in 0 1 2; do
    for t2 in 0 1 2; do
      for t3 in 0 1 2; do
        if [ $t1 = $t2 -o $t1 = $t3 -o $t2 = $t3 ]
        then
            continue
        fi

        t=($t1 $t2 $t3)

        # join type
        for j1 in 0 1 2; do
          for j2 in 0 1 2; do
            for j3 in 0 1 2; do

              # join table and column
              for c1 in 0; do
                for c2 in 0 1; do

                  [ ${t[1]} -gt ${t[$c1]} ] && cn1=${t[1]} || cn1=${t[$c1]}
                  [ ${t[2]} -gt ${t[$c2]} ] && cn2=${t[2]} || cn2=${t[$c2]}

                  echo -n "select t1.*, t2.*, t3.* from" ${tn[${t[0]}]} ${jt[$j1]}
                  echo -n " join" ${tn[${t[1]}]}
                  echo -n " on" ${tn[${t[$c1]}]}.c$cn1 = ${tn[${t[1]}]}.c$cn1 ${jt[$j2]}
                  echo -n " join" ${tn[${t[2]}]}
                  echo -n " on" ${tn[${t[$c2]}]}.c$cn2 = ${tn[${t[2]}]}.c$cn2
                  echo    " order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18;"
                done
              done
            done
          done
        done
      done
    done
  done
elif [ $tables -eq 4 ]; then
  # table id
  for t1 in 0 1 2 3; do
    for t2 in 0 1 2 3; do
      for t3 in 0 1 2 3; do
        for t4 in 0 1 2 3; do
          if [ $t1 = $t2 -o $t1 = $t3 -o $t1 = $t4 -o $t2 = $t3 -o $t2 = $t4 -o $t3 = $t4 ]
          then
            continue
          fi

          t=($t1 $t2 $t3 $t4)

          # join type
          for j1 in 0 1 2; do
            for j2 in 0 1 2; do
              for j3 in 0 1 2; do

                # join table and column
                for c1 in 0; do
                  for c2 in 0 1; do
                    for c3 in 0 1 2; do

                      [ ${t[1]} -gt ${t[$c1]} ] && cn1=${t[1]} || cn1=${t[$c1]}
                      [ ${t[2]} -gt ${t[$c2]} ] && cn2=${t[2]} || cn2=${t[$c2]}
                      [ ${t[3]} -gt ${t[$c3]} ] && cn3=${t[3]} || cn3=${t[$c3]}

                      echo -n "select t1.*, t2.*, t3.*, t4.* from" ${tn[${t[0]}]} ${jt[$j1]}
                      echo -n " join" ${tn[${t[1]}]}
                      echo -n " on" ${tn[${t[$c1]}]}.c$cn1 = ${tn[${t[1]}]}.c$cn1 ${jt[$j2]}
                      echo -n " join" ${tn[${t[2]}]}
                      echo -n " on" ${tn[${t[$c2]}]}.c$cn2 = ${tn[${t[2]}]}.c$cn2 ${jt[$j3]}
                      echo -n " join" ${tn[${t[3]}]}
                      echo -n " on" ${tn[${t[$c3]}]}.c$cn3 = ${tn[${t[3]}]}.c$cn3
                      echo -n " order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,"
                      echo    "19,20,21,22,23,24;"
                    done
                  done
                done
              done
            done
          done
        done
      done
    done
  done
else
  # table id
  for t1 in 0 1 2 3 4; do
    for t2 in 0 1 2 3 4; do
      for t3 in 0 1 2 3 4; do
        for t4 in 0 1 2 3 4; do
          for t5 in 0 1 2 3 4; do
            if [ $t1 = $t2 -o $t1 = $t3 -o $t1 = $t4 -o $t1 = $t5 -o \
                 $t2 = $t3 -o $t2 = $t4 -o $t2 = $t5 -o \
                 $t3 = $t4 -o $t3 = $t5 -o \
                 $t4 = $t5 \
               ]
            then
              continue
            fi

            t=($t1 $t2 $t3 $t4 $t5)

            # join type
            for j1 in 0 1 2; do
              for j2 in 0 1 2; do
                for j3 in 0 1 2; do
                  for j4 in 0 1 2; do

                    # join table and column
                    for c1 in 0; do
                      for c2 in 0 1; do
                        for c3 in 0 1 2; do
                          for c4 in 0 1 2 3; do

                            [ ${t[1]} -gt ${t[$c1]} ] && cn1=${t[1]} || cn1=${t[$c1]}
                            [ ${t[2]} -gt ${t[$c2]} ] && cn2=${t[2]} || cn2=${t[$c2]}
                            [ ${t[3]} -gt ${t[$c3]} ] && cn3=${t[3]} || cn3=${t[$c3]}
                            [ ${t[4]} -gt ${t[$c4]} ] && cn4=${t[4]} || cn4=${t[$c4]}

                            echo -n "select t1.*, t2.*, t3.*, t4.*, t5.* from "
                            echo -n ${tn[${t[0]}]} ${jt[$j1]}
                            echo -n " join" ${tn[${t[1]}]}
                            echo -n " on" ${tn[${t[$c1]}]}.c$cn1 = ${tn[${t[1]}]}.c$cn1 ${jt[$j2]}
                            echo -n " join" ${tn[${t[2]}]}
                            echo -n " on" ${tn[${t[$c2]}]}.c$cn2 = ${tn[${t[2]}]}.c$cn2 ${jt[$j3]}
                            echo -n " join" ${tn[${t[3]}]}
                            echo -n " on" ${tn[${t[$c3]}]}.c$cn3 = ${tn[${t[3]}]}.c$cn3 ${jt[$j4]}
                            echo -n " join" ${tn[${t[4]}]}
                            echo -n " on" ${tn[${t[$c4]}]}.c$cn4 = ${tn[${t[4]}]}.c$cn4
                            echo -n " order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,"
                            echo    "19,20,21,22,23,24,25,26,27,28,29,30;"
                          done
                        done
                      done
                    done
                  done
                done
              done
            done
          done
        done
      done
    done
  done
fi
