color_normal=$(tput sgr0)
color_bold=$(tput bold)
color_red="$color_bold$(tput setaf 1)"
color_green=$(tput setaf 2)
color_fawn=$(tput setaf 3); color_beige="$color_fawn"
color_yellow="$color_bold$color_fawn"
color_darkblue=$(tput setaf 4)
color_blue="$color_bold$color_darkblue"
color_purple=$(tput setaf 5); color_magenta="$color_purple"
color_pink="$color_bold$color_purple"
color_darkcyan=$(tput setaf 6)
color_cyan="$color_bold$color_darkcyan"
color_gray=$(tput setaf 7)
color_darkgray="$color_bold"$(tput setaf 0)
color_white="$color_bold$color_gray"

message()
{
    echo $color_cyan -- $@$color_normal
}

warn()
{
    echo $color_yellow -- $@$color_normal
}

error()
{
    echo $color_red -- $@$color_normal
}

detect_distro()
{
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        export OS=$NAME
        export OS_VERSION=$VERSION_ID
    elif type lsb_release >/dev/null 2>&1; then
        # linuxbase.org
        export OS=$(lsb_release -si)
        export OS_VERSION=$(lsb_release -sr)
    elif [ -f /etc/lsb-release ]; then
        # For some versions of Debian/Ubuntu without lsb_release command
        . /etc/lsb-release
        export OS=$DISTRIB_ID
        OS_VERSION=$DISTRIB_RELEASE
    elif [ -f /etc/debian_version ]; then
        # Older Debian/Ubuntu/etc.
        OS=Debian
        OS_VERSION=$(cat /etc/debian_version)
    else
        # Fall back to uname, e.g. "Linux <version>", also works for BSD, etc.
        OS=$(uname -s)
        OS_VERSION=$(uname -r)
    fi
    OS=$(echo $OS | cut -f 1 -d " ")
    message "Detected $OS $OS_VERSION"
}

menuStr=""

function hideCursor(){
  printf "\033[?25l"

  # capture CTRL+C so cursor can be reset
  trap "showCursor && exit 0" 2
}

function showCursor(){
  printf "\033[?25h"
}

function clearLastMenu(){
  local msgLineCount=$(printf "$menuStr" | wc -l)
  # moves the curser up N lines so the output overwrites it
  echo -en "\033[${msgLineCount}A"

  # clear to end of screen to ensure there's no text left behind from previous input
  [ $1 ] && tput ed
}

function renderMenu(){
  local start=0
  local selector=""
  local instruction="$1"
  local selectedIndex=$2
  local listLength=$itemsLength
  local longest=0
  local spaces=""
  menuStr="\n $instruction\n"

  # Get the longest item from the list so that we know how many spaces to add
  # to ensure there's no overlap from longer items when a list is scrolling up or down.
  for (( i=0; i<$itemsLength; i++ )); do
    if (( ${#menuItems[i]} > longest )); then
      longest=${#menuItems[i]}
    fi
  done
  spaces=$(printf ' %.0s' $(eval "echo {1.."$(($longest))"}"))

  if [ $3 -ne 0 ]; then
    listLength=$3

    if [ $selectedIndex -ge $listLength ]; then
      start=$(($selectedIndex+1-$listLength))
      listLength=$(($selectedIndex+1))
    fi
  fi

  for (( i=$start; i<$listLength; i++ )); do
    local currItem="${menuItems[i]}"
    currItemLength=${#currItem}

    if [[ $i = $selectedIndex ]]; then
      selectedChoice="${currItem}"
      selector="${color_green}ᐅ${color_normal}"
      currItem="${color_green}${currItem}${color_normal}"
    else
      selector=" "
    fi

    currItem="${spaces:0:0}${currItem}${spaces:currItemLength}"

    menuStr="${menuStr}\n ${selector} ${currItem}"
  done

  menuStr="${menuStr}\n"

  # whether or not to overwrite the previous menu output
  [ $4 ] && clearLastMenu

  printf "${menuStr}"
}

function getChoice(){
  local KEY__ARROW_UP=$(echo -e "\033[A")
  local KEY__ARROW_DOWN=$(echo -e "\033[B")
  local KEY__ENTER=$(echo -e "\n")
  local captureInput=true
  local displayHelp=false
  local maxViewable=0
  local instruction="Select an item from the list:"
  local selectedIndex=0

  remainingArgs=()
  while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
      -h|--help)
        displayHelp=true
        shift
        ;;
      -i|--index)
        selectedIndex=$2
        shift 2
        ;;
      -m|--max)
        maxViewable=$2
        shift 2
        ;;
      -o|--options)
        menuItems=$2[@]
        menuItems=("${!menuItems}")
        shift 2
        ;;
      -q|--query)
        instruction="$2"
        shift 2
        ;;
      *)
        remainingArgs+=("$1")
        shift
        ;;
    esac
  done

  # just display help
  if $displayHelp; then
    echo;
    echo "Usage: getChoice [OPTION]..."
    echo "Renders a keyboard navigable menu with a visual indicator of what's selected."
    echo;
    echo "  -h, --help     Displays this message"
    echo "  -i, --index    The initially selected index for the options"
    echo "  -m, --max      Limit how many options are displayed"
    echo "  -o, --options  An Array of options for a User to choose from"
    echo "  -q, --query    Question or statement presented to the User"
    echo;
    echo "Example:"
    echo "  foodOptions=(\"pizza\" \"burgers\" \"chinese\" \"sushi\" \"thai\" \"italian\" \"shit\")"
    echo;
    echo "  getChoice -q \"What do you feel like eating?\" -o foodOptions -i \$((\${#foodOptions[@]}-1)) -m 4"
    echo "  printf \"\\n First choice is '\${selectedChoice}'\\n\""
    echo;
    echo "  getChoice -q \"Select another option in case the first isn't available\" -o foodOptions"
    echo "  printf \"\\n Second choice is '\${selectedChoice}'\\n\""
    echo;

    return 0
  fi

  set -- "${remainingArgs[@]}"
  local itemsLength=${#menuItems[@]}

  # no menu items, at least 1 required
  if [[ $itemsLength -lt 1 ]]; then
    printf "\n [ERROR] No menu items provided\n"
    exit 1
  fi

  renderMenu "$instruction" $selectedIndex $maxViewable
  hideCursor

  while $captureInput; do
    read -rsn3 key # `3` captures the escape (\033'), bracket ([), & type (A) characters.

    case "$key" in
      "$KEY__ARROW_UP")
        selectedIndex=$((selectedIndex-1))
        (( $selectedIndex < 0 )) && selectedIndex=$((itemsLength-1))

        renderMenu "$instruction" $selectedIndex $maxViewable true
        ;;

      "$KEY__ARROW_DOWN")
        selectedIndex=$((selectedIndex+1))
        (( $selectedIndex == $itemsLength )) && selectedIndex=0

        renderMenu "$instruction" $selectedIndex $maxViewable true
        ;;

      "$KEY__ENTER")
        clearLastMenu true
        showCursor
        captureInput=false
        ;;
    esac
  done
}


function optparse.throw_error(){
  local message="$1"
        error "OPTPARSE: ERROR: $message"
        exit 1
}

# -----------------------------------------------------------------------------------------------------------------------------
function optparse.define(){
        if [ $# -lt 3 ]; then
                optparse.throw_error "optparse.define <short> <long> <variable> [<desc>] [<default>] [<value>]"
        fi
        for option_id in $( seq 1 $# ) ; do
                local option="$( eval "echo \$$option_id")"
                local key="$( echo $option | awk -F "=" '{print $1}' )";
                local value="$( echo $option | awk -F "=" '{print $2}' )";

                #essentials: shortname, longname, description
                if [ "$key" = "short" ]; then
                        local shortname="$value"
                        if [ ${#shortname} -ne 1 ]; then
                                optparse.throw_error "short name expected to be one character long"
                        fi
                        local short="-${shortname}"
                elif [ "$key" = "long" ]; then
                        local longname="$value"
                        if [ ${#longname} -lt 2 ]; then
                                optparse.throw_error "long name expected to be atleast one character long"
                        fi
                        local long="--${longname}"
                elif [ "$key" = "desc" ]; then
                        local desc="$value"
                elif [ "$key" = "default" ]; then
                        local default="$value"
                elif [ "$key" = "variable" ]; then
                        local variable="$value"
                elif [ "$key" = "value" ]; then
                        local val="$value"
                fi
        done

        if [ "$variable" = "" ]; then
                optparse.throw_error "You must give a variable for option: ($short/$long)"
        fi

        if [ "$val" = "" ]; then
                val="\$OPTARG"
        fi

        # build OPTIONS and help
        optparse_usage="${optparse_usage}#NL#TB${short} $(printf "%-25s %s" "${long}:" "${desc}")"
        if [ "$default" != "" ]; then
                optparse_usage="${optparse_usage} [default:$default]"
        fi
        optparse_contractions="${optparse_contractions}#NL#TB#TB${long})#NL#TB#TB#TBparams=\"\$params ${short}\";;"
        if [ "$default" != "" ]; then
                optparse_defaults="${optparse_defaults}#NL${variable}=${default}"
        fi
        optparse_arguments_string="${optparse_arguments_string}${shortname}"
        if [ "$val" = "\$OPTARG" ]; then
                optparse_arguments_string="${optparse_arguments_string}:"
        fi
        optparse_process="${optparse_process}#NL#TB#TB${shortname})#NL#TB#TB#TB${variable}=\"$val\";;"
}

# -----------------------------------------------------------------------------------------------------------------------------
function optparse.build(){
        local build_file="$(mktemp -t "optparse-XXXXXX.tmp")"

        # Building getopts header here

        # Function usage
        cat << EOF > $build_file
function usage(){
cat << XXX
usage: \$0 [OPTIONS]
OPTIONS:
        $optparse_usage
        -? --help  :  usage
XXX
}
# Contract long options into short options
params=""
while [ \$# -ne 0 ]; do
        param="\$1"
        shift
        case "\$param" in
                $optparse_contractions
                "-?"|--help)
                        usage
                        exit 0;;
                *)
                        if [[ "\$param" == --* ]]; then
                                echo -e "Unrecognized long option: \$param"
                                usage
                                exit 1
                        fi
                        params="\$params \"\$param\"";;
        esac
done
eval set -- "\$params"
# Set default variable values
$optparse_defaults
# Process using getopts
while getopts "$optparse_arguments_string" option; do
        case \$option in
                # Substitute actions for different variables
                $optparse_process
                :)
                        echo "Option - \$OPTARG requires an argument"
                        exit 1;;
                *)
                        usage
                        exit 1;;
        esac
done
# Clean up after self
rm $build_file
EOF

        local -A o=( ['#NL']='\n' ['#TB']='\t' )

        for i in "${!o[@]}"; do
                sed -i "s/${i}/${o[$i]}/g" $build_file
        done

        # Unset global variables
        unset optparse_usage
        unset optparse_process
        unset optparse_arguments_string
        unset optparse_defaults
        unset optparse_contractions

        # Return file name to parent
        echo "$build_file"
}