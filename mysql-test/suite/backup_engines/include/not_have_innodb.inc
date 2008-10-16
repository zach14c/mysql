# We will disable Innodb storage engine if we include not_have_innodb.inc
# in our test case.


--replace_result $ENGINE ENGINE
if(`SELECT '$ENGINE'='innodb'`)
{
 skip "This test does not support Innodb engine";
}

