--disable_abort_on_error
let $maxscale = 0;
let $maxscalerepl = `SELECT @is_maxscale`;
if ($maxscalerepl == "YES")
{
  let $maxscale = 1;
}

--enable_abort_on_error
