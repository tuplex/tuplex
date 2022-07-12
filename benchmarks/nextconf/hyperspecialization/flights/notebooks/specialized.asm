0000000000001410 <process_cells>:
    1410:	41 56                	push   %r14
    1412:	53                   	push   %rbx
    1413:	50                   	push   %rax
    1414:	48 8b 8e c8 01 00 00 	mov    0x1c8(%rsi),%rcx
    141b:	b8 2a 00 00 00       	mov    $0x2a,%eax
    1420:	80 39 00             	cmpb   $0x0,(%rcx)
    1423:	74 08                	je     142d <process_cells+0x1d>
    1425:	48 83 c4 08          	add    $0x8,%rsp
    1429:	5b                   	pop    %rbx
    142a:	41 5e                	pop    %r14
    142c:	c3                   	retq   
    142d:	4c 8b 76 10          	mov    0x10(%rsi),%r14
    1431:	48 8b 7e 18          	mov    0x18(%rsi),%rdi
    1435:	31 f6                	xor    %esi,%esi
    1437:	ba 0a 00 00 00       	mov    $0xa,%edx
    143c:	e8 cf fc ff ff       	callq  1110 <strtol@plt>
    1441:	48 89 c3             	mov    %rax,%rbx
    1444:	4c 89 f7             	mov    %r14,%rdi
    1447:	31 f6                	xor    %esi,%esi
    1449:	ba 0a 00 00 00       	mov    $0xa,%edx
    144e:	e8 bd fc ff ff       	callq  1110 <strtol@plt>
    1453:	c1 e3 04             	shl    $0x4,%ebx
    1456:	01 c3                	add    %eax,%ebx
    1458:	48 8b 05 61 2c 00 00 	mov    0x2c61(%rip),%rax        # 40c0 <_ZL7agg_map>
    145f:	48 63 cb             	movslq %ebx,%rcx
    1462:	48 c1 e1 05          	shl    $0x5,%rcx
    1466:	80 3c 08 00          	cmpb   $0x0,(%rax,%rcx,1)
    146a:	74 07                	je     1473 <process_cells+0x63>
    146c:	48 ff 44 08 08       	incq   0x8(%rax,%rcx,1)
    1471:	eb 10                	jmp    1483 <process_cells+0x73>
    1473:	48 8d 14 08          	lea    (%rax,%rcx,1),%rdx
    1477:	c6 02 01             	movb   $0x1,(%rdx)
    147a:	48 c7 44 08 08 00 00 	movq   $0x0,0x8(%rax,%rcx,1)
    1481:	00 00 
    1483:	31 c0                	xor    %eax,%eax
    1485:	48 83 c4 08          	add    $0x8,%rsp
    1489:	5b                   	pop    %rbx
    148a:	41 5e                	pop    %r14
    148c:	c3                   	retq   
    148d:	0f 1f 00             	nopl   (%rax)
