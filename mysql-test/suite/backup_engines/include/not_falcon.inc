# We will disable Falcon storage engine if we include not_falcon.inc
# in our test case.

--replace_result $ENGINE ENGINE
if(`SELECT '$ENGINE'='falcon'`)
{
 skip "This test does not support Falcon engine";
}

