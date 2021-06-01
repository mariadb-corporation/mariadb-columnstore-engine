package My::Suite::ColumnStore;

@ISA = qw(My::Suite);

my $mcs_bin_dir_compiled=$::bindir . '/storage/columnstore/columnstore/bin';
my $mcs_ins_dir_installed=$::bindir . '/bin';

if (-d $mcs_bin_dir_compiled)
{
  $ENV{MCS_MCSSETCONFIG}=$mcs_bin_dir_compiled . "/mcsSetConfig";
  $ENV{MCS_CPIMPORT}=$mcs_bin_dir_compiled . "/cpimport";
}
elsif (-d $mcs_ins_dir_installed)
{
  $ENV{MCS_MCSSETCONFIG}=$mcs_ins_dir_installed . "/mcsSetConfig";
  $ENV{MCS_CPIMPORT}=$mcs_ins_dir_installed . "/cpimport";
}

sub is_default { 0 }

bless { };
