--disable_abort_on_error

let $maxscale = `SELECT @is_maxscale`;

if ($maxscale == "YES")
{
  --skip Test can not be used with Maxscale.
}

--enable_abort_on_error
