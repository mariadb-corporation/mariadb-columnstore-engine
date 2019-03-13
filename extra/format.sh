#!/bin/bash
astyle --style=allman --indent=spaces=4 --indent-switches --break-blocks --pad-comma --pad-oper --pad-header --lineend=linux --align-pointer=type --recursive "*.cpp" "*.h"
