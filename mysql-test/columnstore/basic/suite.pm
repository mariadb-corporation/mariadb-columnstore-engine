package My::Suite::ColumnStore;

@ISA = qw(My::Suite);

my $mcs_bin_dir_compiled=$::bindir . '/storage/columnstore/columnstore/bin';
my $mcs_ins_dir_installed=$::bindir . '/bin';

if (-d $mcs_bin_dir_compiled)
{
  $ENV{MCS_MCSSETCONFIG}=$mcs_bin_dir_compiled . "/mcsSetConfig";
  $ENV{MCS_CPIMPORT}=$mcs_bin_dir_compiled . "/cpimport";
  $ENV{MCS_SYSCATALOG_MYSQL_SQL}=$::mysqld_variables{'basedir'} . "/storage/columnstore/columnstore/dbcon/mysql/syscatalog_mysql.sql";
}
elsif (-d $mcs_ins_dir_installed)
{
  $ENV{MCS_MCSSETCONFIG}=$mcs_ins_dir_installed . "/mcsSetConfig";
  $ENV{MCS_CPIMPORT}=$mcs_ins_dir_installed . "/cpimport";
  $ENV{MCS_SYSCATALOG_MYSQL_SQL}=$::mysqld_variables{'basedir'} . "/share/columnstore/syscatalog_mysql.sql";
}

sub is_default { 0 }

bless { };
