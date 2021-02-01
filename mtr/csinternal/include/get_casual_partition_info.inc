perl;
use strict;
use warnings;
#
echo '------------'
my $oidFile =  '/tmp/mcpoids.txt';
my $infoFile =  '/tmp/mcpinfo.txt';

open(FHoid, '<', $oidFile) or die $!;
open(FHinfo, '>>', $infoFile) or die $!;

while(<FHoid>){
   my $info = `/usr/bin/editem -o $_ `;
   print FHinfo $info;
}
close(FHinfo);
close(FHoid);
EOF
