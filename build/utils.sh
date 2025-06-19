#!/bin/bash

if [[ -n "$TERM" && "$TERM" != "dumb" && $(command -v tput) ]]; then
  TPUT_AVAILABLE=true
else
  TPUT_AVAILABLE=false
fi

if [[ $TPUT_AVAILABLE == true ]]; then
  color_normal=$(tput sgr0)
  color_bold=$(tput bold)
  color_red="$color_bold$(tput setaf 1)"
  color_green="$color_bold$(tput setaf 2)"
  color_fawn=$(tput setaf 3)
  color_beige="$color_fawn"
  color_yellow="$color_bold$color_fawn"
  color_darkblue=$(tput setaf 4)

  color_blue="$color_bold$color_darkblue"
  color_purple=$(tput setaf 5)
  color_magenta="$color_purple"
  color_pink="$color_bold$color_purple"
  color_darkcyan=$(tput setaf 6)
  color_cyan="$color_bold$color_darkcyan"
  color_gray=$(tput setaf 7)
  color_darkgray="$color_bold"$(tput setaf 0)
  color_white="$color_bold$color_gray"

  if [[ $(tput colors) == '256' ]]; then
    color_red=$(tput setaf 196)
    color_yellow=$(tput setaf 228)
    color_cyan=$(tput setaf 87)
    color_green=$(tput setaf 156)
    color_darkgray=$(tput setaf 59)
  fi

else
  # Basic attributes
  color_normal="\e[0m" # sgr0
  color_bold="\e[1m"   # bold

  # Standard 8-color palette
  color_red="\e[1;31m"          # bold + setaf 1
  color_green="\e[1;32m"        # bold + setaf 2
  color_fawn="\e[33m"           # setaf 3
  color_beige="$color_fawn"     # alias
  color_yellow="\e[1;33m"       # bold + setaf 3
  color_darkblue="\e[34m"       # setaf 4
  color_blue="\e[1;34m"         # bold + setaf 4
  color_purple="\e[35m"         # setaf 5
  color_magenta="$color_purple" # alias
  color_pink="\e[1;35m"         # bold + setaf 5
  color_darkcyan="\e[36m"       # setaf 6
  color_cyan="\e[1;36m"         # bold + setaf 6
  color_gray="\e[37m"           # setaf 7
  color_darkgray="\e[1;30m"     # bold + setaf 0
  color_white="\e[1;37m"        # bold + setaf 7
  if [ "$TERM" = "xterm-256color" ] || [ "$COLORTERM" = "truecolor" ] || [ "$COLORTERM" = "24bit" ]; then
    # Only set 256-color codes if actually in a 256-color terminal
    color_red="\e[91m"      # bright red
    color_yellow="\e[93m"   # light yellow
    color_cyan="\e[96m"     # bright cyan
    color_green="\e[92m"    # light green
    color_darkgray="\e[90m" # dark gray
  fi
fi

message() {
  echo -e $color_cyan ・ $@$color_normal
}

warn() {
  echo -e $color_yellow ・ $@$color_normal
}

error() {
  echo -e $color_red ・ $@$color_normal
}

message_split() {
  echo -e $color_darkgray ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ $color_normal
}

message_splitted() {
  message_split
  if [[ $TPUT_AVAILABLE == true ]]; then
    echo $color_green ・ $@$color_normal
  else
    echo "$@"
  fi
  message_split
}

colorify_array() {
  PROMT=""
  for a in "$@"; do
    if [[ $TPUT_AVAILABLE == true ]]; then
      i=$((((i + 1) % (123 - 106)) + 106))
      if [[ $(tput colors) == '256' ]]; then
        PROMT="$PROMT $(tput setaf $i)$a$color_normal"
      else
        PROMT="$PROMT $a"
      fi
    else
      PROMT="$PROMT $a"
    fi
  done
  echo $PROMT
}

newline_array() {
  PROMT=""
  for a in "$@"; do
    PROMT="$PROMT$a\n"
  done
  echo -e $PROMT
}

function spinner() {
  freq=${1:-10}
  points=(⣾ ⣽ ⣻ ⢿ ⡿ ⣟ ⣯ ⣷)
  colored_points=($(colorify_array ${points[@]}))
  len=${#points[@]}
  point_num=0
  line_num=0
  while read data; do
    line_num=$((line_num + 1))
    if [[ $((line_num % freq)) = 0 ]]; then
      point_num=$(((point_num + 1) % len))
      if [[ $TPUT_AVAILABLE == true ]]; then
        echo -ne "\r${colored_points[point_num]}"
      else
        echo -ne "\r${points[point_num]}"
      fi
    fi
  done
  echo
}

function onelinearizator() {
  while read data; do
    echo -ne "\r\e[K$data"
  done
  echo
}

detect_distro() {
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
  OS=$(echo $OS | cut -f 1 -d " " | tr '[:upper:]' '[:lower:]')":"$OS_VERSION
  message "Detected $color_yellow$OS $OS_VERSION$color_normal"
}

menuStr=""

function hideCursor() {
  printf "\e[?25l"

  # capture CTRL+C so cursor can be reset
  trap "showCursor && exit 0" 2
}

function showCursor() {
  printf "\e[?25h"
}

function clearLastMenu() {
  local msgLineCount=$(printf "$menuStr" | wc -l)
  # moves the curser up N lines so the output overwrites it
  echo -en "\e[${msgLineCount}A"

  # clear to end of screen to ensure there's no text left behind from previous input
  [ $1 ] && tput ed
}

function renderMenu() {
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
  for ((i = 0; i < $itemsLength; i++)); do
    if ((${#menuItems[i]} > longest)); then
      longest=${#menuItems[i]}
    fi
  done
  spaces=$(printf ' %.0s' $(eval "echo {1.."$(($longest))"}"))

  if [ $3 -ne 0 ]; then
    listLength=$3

    if [ $selectedIndex -ge $listLength ]; then
      start=$(($selectedIndex + 1 - $listLength))
      listLength=$(($selectedIndex + 1))
    fi
  fi

  for ((i = $start; i < $listLength; i++)); do
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

function getChoice() {
  local KEY__ARROW_UP=$(echo -e "\e[A")
  local KEY__ARROW_DOWN=$(echo -e "\e[B")
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
    -h | --help)
      displayHelp=true
      shift
      ;;
    -i | --index)
      selectedIndex=$2
      shift 2
      ;;
    -m | --max)
      maxViewable=$2
      shift 2
      ;;
    -o | --options)
      menuItems=$2[@]
      menuItems=("${!menuItems}")
      shift 2
      ;;
    -q | --query)
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
    echo
    echo "Usage: getChoice [OPTION]..."
    echo "Renders a keyboard navigable menu with a visual indicator of what's selected."
    echo
    echo "  -h, --help     Displays this message"
    echo "  -i, --index    The initially selected index for the options"
    echo "  -m, --max      Limit how many options are displayed"
    echo "  -o, --options  An Array of options for a User to choose from"
    echo "  -q, --query    Question or statement presented to the User"
    echo
    echo "Example:"
    echo "  foodOptions=(\"pizza\" \"burgers\" \"chinese\" \"sushi\" \"thai\" \"italian\" \"shit\")"
    echo
    echo "  getChoice -q \"What do you feel like eating?\" -o foodOptions -i \$((\${#foodOptions[@]}-1)) -m 4"
    echo "  printf \"\\n First choice is '\${selectedChoice}'\\n\""
    echo
    echo "  getChoice -q \"Select another option in case the first isn't available\" -o foodOptions"
    echo "  printf \"\\n Second choice is '\${selectedChoice}'\\n\""
    echo

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
    read -rsn3 key # `3` captures the escape (\e'), bracket ([), & type (A) characters.

    case "$key" in
    "$KEY__ARROW_UP")
      selectedIndex=$((selectedIndex - 1))
      (($selectedIndex < 0)) && selectedIndex=$((itemsLength - 1))

      renderMenu "$instruction" $selectedIndex $maxViewable true
      ;;

    "$KEY__ARROW_DOWN")
      selectedIndex=$((selectedIndex + 1))
      (($selectedIndex == $itemsLength)) && selectedIndex=0

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

function optparse.throw_error() {
  local message="$1"
  error "OPTPARSE: ERROR: $message"
  exit 1
}

# -----------------------------------------------------------------------------------------------------------------------------
function optparse.define() {
  if [ $# -lt 3 ]; then
    optparse.throw_error "optparse.define <short> <long> <variable> [<desc>] [<default>] [<value>]"
  fi
  for option_id in $(seq 1 $#); do
    local option="$(eval "echo \$$option_id")"
    local key="$(echo $option | awk -F "=" '{print $1}')"
    local value="$(echo $option | awk -F "=" '{print $2}')"

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
function optparse.build() {
  local build_file="$(mktemp "${TMPDIR:-/tmp}/optparse-XXXXXX")"

  # Building getopts header here

  # Function usage
  cat <<EOF >$build_file
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

  local -A o=(['#NL']='\n' ['#TB']='\t')

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

function retry_eval() {
  if [ "$#" -lt 2 ]; then
    error "Usage: retry_eval <max_retries> <command...>"
    return 1
  fi

  local max_retries=$1
  shift # Remove max_retries from arguments
  local attempt=1
  local initial_delay=1

  while [ "$attempt" -le "$max_retries" ]; do
    message_split
    message "Attempt $attempt of $max_retries: $*"
    if eval "$@"; then
      message "Command '$@' done"
      message_split
      return 0
    fi
    if [ "$attempt" -lt "$max_retries" ]; then
      delay=$((initial_delay * 2 ** (attempt - 1)))
      warn "Retrying command "$@" in $delay seconds..."
      message_split
      sleep "$delay"
    fi
    ((attempt++))
  done

  error "Max retries reached for command: $*"
  message_split
  exit 13
}

function execInnerDocker() {
  local container_name=$1
  shift 1 # Remove first arg (container_name)

  docker exec -t "$container_name" bash -c "$@"
  local dockerCommandExitCode=$?

  if [[ $dockerCommandExitCode -ne 0 ]]; then
    error "Command \"$@\" failed in container \"$container_name\""
    exit $dockerCommandExitCode
  fi
}

function execInnerDockerNoTTY() {
  local container_name=$1
  shift 1

  docker exec "$container_name" bash -c "$@"
  local dockerCommandExitCode=$?

  if [[ $dockerCommandExitCode -ne 0 ]]; then
    error "Command \"$@\" failed in container \"$container_name\""
    exit $dockerCommandExitCode
  fi
}

function change_ubuntu_mirror() {
  local region="$1"
  message "Changing Ubuntu mirror to $region"
  sed -i "s|//\(${region}\.\)\?archive\.ubuntu\.com|//${region}.archive.ubuntu.com|g" /etc/apt/sources.list 2>/dev/null || true
  sed -i "s|//\(${region}\.\)\?archive\.ubuntu\.com|//${region}.archive.ubuntu.com|g" /etc/apt/sources.list.d/ubuntu.sources 2>/dev/null || true
  cat /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list 2>/dev/null | grep archive || true
  message_split
}

function execInnerDockerWithRetry() {
  local max_retries=5
  local container_name=$1
  shift 1

  local cmd=("$@")
  local attempt=1
  local dockerCommandExitCode=0

  local docker_funcs=$(declare -f retry_eval color_normal color_cyan color_yellow color_red error warn message message_split)

  # Build the full command to execute in docker
  local full_command="$docker_funcs; retry_eval $max_retries \"${cmd[*]}\""

  # Execute the command in docker
  docker exec -t "$container_name" bash -c "$full_command"
  dockerCommandExitCode=$?

  if [[ $dockerCommandExitCode -ne 0 ]]; then
    error "Command \"${cmd[*]}\" failed in container \"$container_name\" after $max_retries attempts"
    return $dockerCommandExitCode
  fi

  return 0
}

change_ubuntu_mirror_in_docker() {
  local container_name=$1
  local region=$2
  local docker_funcs=$(declare -f color_normal color_cyan color_yellow color_red error warn message message_split change_ubuntu_mirror)

  execInnerDocker "$container_name" "$docker_funcs; change_ubuntu_mirror ${region}"
}
