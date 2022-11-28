
-- error 0,1
perl;
my $path= $ENV{'MYSQL_TMP_DIR'};
exit 1 unless open OUT, ">", "$path/sparse_test";
exit 1 unless seek(OUT, 0x8000, 0);
print OUT "some text";
close OUT or exit 1;
EOF
let $err= $errno;

-- error 0,1
-- remove_file $MYSQL_TMP_DIR/sparse_test
if ($err)
{
  -- skip Test requires sparse file support
}
