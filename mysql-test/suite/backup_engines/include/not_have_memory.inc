# We will disable Memory storage engine if we include not_have_memory.inc
# in our test case.


--replace_result $ENGINE ENGINE
if(`SELECT '$ENGINE'='memory'`)
{
 skip "This test does not support memory engine";
}

