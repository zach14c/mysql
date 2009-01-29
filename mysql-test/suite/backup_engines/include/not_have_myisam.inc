# We will disable Myisam storage engine if we include not_have_myisam.inc
# in our test case.


--replace_result $ENGINE ENGINE
if(`SELECT '$ENGINE'='myisam'`)
{
 skip "This test does not support Myisam engine";
}

