bash -c "tee >(sed $'s/\033[[][^A-Za-z]*m//g' > $1)"

