--disable_abort_on_error

let $maxscale = `SELECT @is_maxscale`;

if ($maxscale != "YES")
{
  --skip Test requires connection via Maxscale.
}

--enable_abort_on_error
